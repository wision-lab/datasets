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
        **kwargs,
    ) -> None:
        """
        Args:
            auto_visible (bool, optional): If true, chunk bars are hidden until a
                worker first updates them and are hidden again once it finishes.
                Defaults to True.
            description (str, optional): Description shown on the overall bar.
        """
        self.overall_taskid: TaskID | None = None
        self.inflight_tasks: set[TaskID] = set()
        self.completed_tasks: set[TaskID] = set()
        self.auto_visible = auto_visible
        self.description = description
        super().__init__(*args, **kwargs)

    @classmethod
    def get_default_columns(cls) -> tuple[ProgressColumn, ...]:
        """Columns matching the rest of the script (elapsed time when finished)."""
        return (
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(elapsed_when_finished=True),
        )

    def __enter__(self) -> Self:
        self.start()
        self.overall_taskid = super().add_task(self.description, total=0)
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
        """Refresh the overall bar from finished chunks plus in-flight fractions."""
        if self.overall_taskid is None:
            return
        inflight = sum(self._tasks[t].percentage / 100 for t in self.inflight_tasks)
        super().update(
            self.overall_taskid,
            completed=len(self.completed_tasks) + inflight,
            total=len(self.completed_tasks) + len(self.inflight_tasks),
        )
