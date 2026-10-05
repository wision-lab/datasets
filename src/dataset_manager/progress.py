from __future__ import annotations

from functools import partial
from types import TracebackType
from typing import Protocol, Self

from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)


class UpdateFn(Protocol):
    """Mirrors `rich.progress.Progress.update` with curried task-id argument"""

    def __call__(
        self,
        total: float | None = None,
        completed: float | None = None,
        advance: float | None = None,
        description: str | None = None,
        visible: bool | None = None,
        refresh: bool = False,
    ) -> None: ...


class UploadProgress(Progress):
    """Progress display for concurrently zipped/uploaded chunks.

    A port of the `PoolProgress` helper used elsewhere in the project, adapted to
    threads: it renders an aggregate "Overall progress" bar alongside one bar per
    in-flight chunk (never more than the number of workers). Chunk bars are
    hidden until a worker starts them and hidden again once they finish, so the
    display stays bounded regardless of how many archives there are.

    Unlike the multiprocessing original this needs no `Manager`/`Queue`: worker
    threads update `rich` directly, since `Progress.update`/`advance` are already
    lock-protected.
    """

    def __init__(
        self,
        *args,
        auto_visible: bool = True,
        description: str = "[green]Overall progress:",
        total: float = 0,
        derive_overall: bool = True,
        show_percent: bool = True,
        **kwargs,
    ) -> None:
        """
        Args:
            auto_visible (bool, optional): If true, chunk bars are hidden until a
                worker first updates them and are hidden again once it finishes.
                Defaults to True.
            description (str, optional): Description shown on the overall bar.
            total (float, optional): Units of work the overall bar should be sized
                against. When 0 the total is derived from the rows seen so far,
                which is the only option for a caller that discovers its work as
                it runs, but leaves the bar's fill meaningless until the work is
                known. Callers that know the count up front pass it.
            derive_overall (bool, optional): Whether the overall bar advances by
                itself as chunk rows finish. True when one row is one unit of work
                (upload). False when the caller's unit is not a row — `check-gz`
                spends two hashing rows per pair, so a row-derived bar would run
                to 100% after half the pairs — in which case the caller drives the
                overall row itself.
            show_percent (bool, optional): Whether to render a percentage column.
                A caller that puts its own count in the description turns this off
                rather than showing the same progress twice.
        """
        self.overall_taskid: TaskID | None = None
        self.overall_total = total
        self.derive_overall = derive_overall
        self.inflight_tasks: set[TaskID] = set()
        self.completed_tasks: set[TaskID] = set()
        self.auto_visible = auto_visible
        self.description = description
        super().__init__(*args, **kwargs)
        if not show_percent:
            self.columns = tuple(column for column in self.columns if not isinstance(column, TaskProgressColumn))

    @classmethod
    def get_default_columns(cls) -> tuple[ProgressColumn, ...]:
        """Columns matching the rest of the script (elapsed time when finished)."""
        return (
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(elapsed_when_finished=True),
        )

    @property
    def overall_task(self) -> TaskID:
        """The overall row's id. Only valid inside the `with` block.

        The attribute is `None` before `__enter__`; callers that need the id (to
        drive the row themselves) use this rather than reaching into the attribute
        and asserting. A real raise, not `assert`: this guard must survive `-O`.
        """
        if self.overall_taskid is None:
            raise RuntimeError("the progress display has not been entered")
        return self.overall_taskid

    def __enter__(self) -> Self:
        self.start()
        self.overall_taskid = super().add_task(self.description, total=self.overall_total)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        super().__exit__(exc_type, exc_val, exc_tb)

    def add_task(self, *args, **kwargs) -> UpdateFn:  # type: ignore[override]
        """Add a chunk task, returning a curried callback used to update it.

        The task is created hidden (when `auto_visible`) and only becomes visible
        once a worker calls the returned callback for the first time.
        """
        if self.auto_visible:
            kwargs["visible"] = False
        task_id = super().add_task(*args, **kwargs)
        with self._lock:
            self.inflight_tasks.add(task_id)
            self._update_overall()
        return partial(self.update, task_id)

    def update(self, task_id: TaskID, **kwargs) -> None:  # type: ignore[override]
        """Update a task, auto-showing it and folding completions into the overall bar.

        A chunk is considered finished when its callback is called with
        `visible=False`, which is what worker threads do once they are done.
        """
        kwargs.setdefault("visible", True)
        super().update(task_id, **kwargs)
        with self._lock:
            if task_id == self.overall_taskid:
                return
            if kwargs.get("visible") is False:
                self.inflight_tasks.discard(task_id)
                self.completed_tasks.add(task_id)
            self._update_overall()

    def _update_overall(self) -> None:
        """Refresh the overall bar from finished chunks plus in-flight fractions.

        Skipped entirely when `derive_overall` is false: a caller whose unit of
        work is not a row drives the overall row itself, because counting rows
        here would run the bar ahead of its own description.
        """
        if self.overall_taskid is None or not self.derive_overall:
            return
        inflight = sum(self._tasks[t].percentage / 100 for t in self.inflight_tasks)
        completed = len(self.completed_tasks) + inflight
        total = self.overall_total or len(self.completed_tasks) + len(self.inflight_tasks)
        super().update(self.overall_taskid, completed=completed, total=max(total, completed, 1))
