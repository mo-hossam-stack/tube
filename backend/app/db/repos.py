from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from sqlalchemy import Select, delete, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.db.tables import CollectionRow, DownloadRow, SettingsRow, utcnow
from app.domain.enums import ACTIVE_STATUSES, Event, Status
from app.domain.state import InvalidTransition, apply

# Each retry means another transition committed in between; a handful is plenty.
_TRANSITION_ATTEMPTS = 8


class StaleRun(InvalidTransition):
    """A worker's run tried to change a job that has been retried since it started."""

    def __str__(self) -> str:
        return f"run superseded by a retry ({self.event.value} while {self.current.value})"


def _filter_downloads(
    stmt: Select,
    status: Status | None,
    collection_id: str | None,
    standalone: bool,
) -> Select:
    if status:
        stmt = stmt.where(DownloadRow.status == status.value)
    if collection_id:
        stmt = stmt.where(DownloadRow.collection_id == collection_id)
    elif standalone:
        stmt = stmt.where(DownloadRow.collection_id.is_(None))
    return stmt


class DownloadRepo:
    def __init__(self, sessions: async_sessionmaker):
        self.sessions = sessions

    async def create(self, **fields: Any) -> DownloadRow:
        async with self.sessions.begin() as session:
            row = DownloadRow(**fields)
            session.add(row)
        return row

    async def get(self, download_id: str) -> DownloadRow | None:
        async with self.sessions() as session:
            return await session.get(DownloadRow, download_id)

    async def list_rows(
        self,
        status: Status | None = None,
        limit: int = 50,
        offset: int = 0,
        collection_id: str | None = None,
        standalone: bool = False,
    ) -> list[DownloadRow]:
        """Newest first, or in playlist order when listing one collection's items."""
        order = (
            (DownloadRow.playlist_index.asc(), DownloadRow.created_at.asc())
            if collection_id
            else (DownloadRow.created_at.desc(),)
        )
        stmt = _filter_downloads(select(DownloadRow), status, collection_id, standalone)
        stmt = stmt.order_by(*order).limit(limit).offset(offset)
        async with self.sessions() as session:
            return list((await session.execute(stmt)).scalars().all())

    async def active(self) -> list[DownloadRow]:
        # Ordered so recovery re-queues jobs, and playlist items, in their original order.
        stmt = (
            select(DownloadRow)
            .where(
                DownloadRow.status.in_([s.value for s in Status if s not in {
                    Status.COMPLETED, Status.FAILED, Status.CANCELED
                }])
            )
            .order_by(DownloadRow.created_at.asc(), DownloadRow.playlist_index.asc())
        )
        async with self.sessions() as session:
            return list((await session.execute(stmt)).scalars().all())

    async def transition(
        self,
        download_id: str,
        event: Event,
        run_attempt: int | None = None,
        **fields: Any,
    ) -> DownloadRow:
        """The only place status is written. Status and fields commit together.

        Compare-and-set: the update only applies if the status is still the one the
        transition was checked against. If a concurrent transition got there first, the
        event is re-checked against the new status, so e.g. a spawn can never overwrite a
        cancel that landed in between (it raises InvalidTransition instead).

        A worker passes ``run_attempt`` (the job's ``attempts`` when it claimed it): once
        the job has been retried, that run can no longer change it and gets StaleRun.
        """
        for _ in range(_TRANSITION_ATTEMPTS):
            async with self.sessions() as session:
                found = (
                    await session.execute(
                        select(DownloadRow.status, DownloadRow.attempts).where(
                            DownloadRow.id == download_id
                        )
                    )
                ).one_or_none()
            if found is None:
                raise KeyError(download_id)
            current, attempts = found
            if run_attempt is not None and attempts != run_attempt:
                raise StaleRun(Status(current), event)
            target = apply(Status(current), event).value
            async with self.sessions.begin() as session:
                result = await session.execute(
                    update(DownloadRow)
                    .where(
                        DownloadRow.id == download_id,
                        DownloadRow.status == current,
                        DownloadRow.attempts == attempts,
                    )
                    .values(status=target, **fields)
                )
                if result.rowcount == 1:
                    return await session.get(DownloadRow, download_id)
        raise RuntimeError(f"status of {download_id} kept changing during {event.value}")

    async def claim(self, download_id: str, event: Event, **fields: Any) -> DownloadRow | None:
        """Atomically move a queued job forward.

        Returns ``None`` when the job is no longer queued (another worker claimed it, or it
        was canceled in the meantime), so exactly one caller ever starts a given job.
        """
        target = apply(Status.QUEUED, event).value
        async with self.sessions.begin() as session:
            result = await session.execute(
                update(DownloadRow)
                .where(DownloadRow.id == download_id, DownloadRow.status == Status.QUEUED.value)
                .values(status=target, **fields)
            )
            if result.rowcount != 1:
                return None
            return await session.get(DownloadRow, download_id)

    async def update_progress(
        self, download_id: str, run_attempt: int | None = None, **fields: Any
    ) -> None:
        """Write non-status fields; a superseded run's writes (see transition) are dropped."""
        stmt = update(DownloadRow).where(DownloadRow.id == download_id)
        if run_attempt is not None:
            stmt = stmt.where(DownloadRow.attempts == run_attempt)
        async with self.sessions.begin() as session:
            await session.execute(stmt.values(**fields))

    async def delete(self, download_id: str) -> None:
        async with self.sessions.begin() as session:
            await session.execute(delete(DownloadRow).where(DownloadRow.id == download_id))

    async def count(
        self,
        status: Status | None = None,
        collection_id: str | None = None,
        standalone: bool = False,
    ) -> int:
        stmt = _filter_downloads(
            select(func.count()).select_from(DownloadRow), status, collection_id, standalone
        )
        async with self.sessions() as session:
            return int((await session.execute(stmt)).scalar_one())

    async def stats(self) -> dict:
        week_ago = utcnow() - timedelta(days=7)
        async with self.sessions() as session:
            counts = dict(
                (
                    await session.execute(
                        select(DownloadRow.status, func.count()).group_by(DownloadRow.status)
                    )
                ).all()
            )
            bytes_week = (
                await session.execute(
                    select(func.coalesce(func.sum(DownloadRow.file_size), 0)).where(
                        DownloadRow.status == Status.COMPLETED.value,
                        DownloadRow.finished_at >= week_ago,
                    )
                )
            ).scalar_one()
            total_completed = (
                await session.execute(
                    select(func.count()).where(DownloadRow.status == Status.COMPLETED.value)
                )
            ).scalar_one()
        return {
            "counts": counts,
            "bytes_last_7d": int(bytes_week or 0),
            "completed_total": int(total_completed or 0),
            "active_total": sum(
                counts.get(s.value, 0)
                for s in (
                    Status.QUEUED,
                    Status.EXTRACTING,
                    Status.DOWNLOADING,
                    Status.PROCESSING,
                    Status.VERIFYING,
                )
            ),
        }


    async def existing_videos(
        self, mode: str, video_ids: Iterable[str]
    ) -> set[tuple[str | None, str]]:
        """(site, video_id) pairs already completed or still in progress in ``mode``."""
        ids = list(set(video_ids))
        if not ids:
            return set()
        wanted = [Status.COMPLETED.value, *(s.value for s in ACTIVE_STATUSES)]
        stmt = select(DownloadRow.site, DownloadRow.video_id).where(
            DownloadRow.mode == mode,
            DownloadRow.status.in_(wanted),
            DownloadRow.video_id.in_(ids),
        )
        async with self.sessions() as session:
            return {(site, vid) for site, vid in (await session.execute(stmt)).all()}

    async def ids_in_collection(self, collection_id: str, statuses: Iterable[Status]) -> list[str]:
        stmt = (
            select(DownloadRow.id)
            .where(
                DownloadRow.collection_id == collection_id,
                DownloadRow.status.in_([s.value for s in statuses]),
            )
            .order_by(DownloadRow.playlist_index.asc())
        )
        async with self.sessions() as session:
            return list((await session.execute(stmt)).scalars().all())


@dataclass
class CollectionSummary:
    row: CollectionRow
    counts: dict[str, int] = field(default_factory=dict)
    active_progress: float = 0.0  # sum of ``progress`` over items still in progress

    @property
    def item_total(self) -> int:
        return sum(self.counts.values())

    def count(self, *statuses: Status) -> int:
        return sum(self.counts.get(s.value, 0) for s in statuses)


class CollectionRepo:
    def __init__(self, sessions: async_sessionmaker):
        self.sessions = sessions

    async def create_with_items(
        self, collection: dict[str, Any], items: list[dict[str, Any]]
    ) -> tuple[CollectionRow, list[DownloadRow]]:
        """Insert a collection and all of its download rows in one transaction."""
        async with self.sessions.begin() as session:
            row = CollectionRow(**collection)
            session.add(row)
            await session.flush()
            downloads = [DownloadRow(collection_id=row.id, **item) for item in items]
            session.add_all(downloads)
        return row, downloads

    async def get(self, collection_id: str) -> CollectionRow | None:
        async with self.sessions() as session:
            return await session.get(CollectionRow, collection_id)

    async def count(self) -> int:
        async with self.sessions() as session:
            stmt = select(func.count()).select_from(CollectionRow)
            return int((await session.execute(stmt)).scalar_one())

    async def summaries(self, limit: int = 50, offset: int = 0) -> list[CollectionSummary]:
        stmt = (
            select(CollectionRow)
            .order_by(CollectionRow.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        async with self.sessions() as session:
            rows = list((await session.execute(stmt)).scalars().all())
        return await self._summarise(rows)

    async def summary(self, collection_id: str) -> CollectionSummary | None:
        row = await self.get(collection_id)
        if row is None:
            return None
        return (await self._summarise([row]))[0]

    async def _summarise(self, rows: list[CollectionRow]) -> list[CollectionSummary]:
        summaries = {row.id: CollectionSummary(row=row) for row in rows}
        if not summaries:
            return []
        stmt = (
            select(
                DownloadRow.collection_id,
                DownloadRow.status,
                func.count(),
                func.coalesce(func.sum(DownloadRow.progress), 0.0),
            )
            .where(DownloadRow.collection_id.in_(list(summaries)))
            .group_by(DownloadRow.collection_id, DownloadRow.status)
        )
        active = {s.value for s in ACTIVE_STATUSES}
        async with self.sessions() as session:
            for collection_id, status, count, progress in (await session.execute(stmt)).all():
                summary = summaries[collection_id]
                summary.counts[status] = int(count)
                if status in active:
                    summary.active_progress += float(progress)
        return [summaries[row.id] for row in rows]

    async def delete(self, collection_id: str) -> None:
        """Remove a collection together with its download rows."""
        async with self.sessions.begin() as session:
            await session.execute(
                delete(DownloadRow).where(DownloadRow.collection_id == collection_id)
            )
            await session.execute(delete(CollectionRow).where(CollectionRow.id == collection_id))

    async def delete_if_empty(self, collection_id: str) -> bool:
        async with self.sessions.begin() as session:
            remaining = (
                await session.execute(
                    select(func.count())
                    .select_from(DownloadRow)
                    .where(DownloadRow.collection_id == collection_id)
                )
            ).scalar_one()
            if remaining:
                return False
            result = await session.execute(
                delete(CollectionRow).where(CollectionRow.id == collection_id)
            )
            return result.rowcount == 1


class SettingsRepo:
    def __init__(self, sessions: async_sessionmaker):
        self.sessions = sessions

    async def get_data(self) -> dict:
        async with self.sessions() as session:
            row = await session.get(SettingsRow, 1)
            return dict(row.data) if row else {}

    async def put_data(self, data: dict) -> None:
        async with self.sessions.begin() as session:
            row = await session.get(SettingsRow, 1)
            if row is None:
                session.add(SettingsRow(id=1, data=data))
            else:
                row.data = data
