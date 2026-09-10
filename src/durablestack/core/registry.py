"""In-process job registration registry."""

from __future__ import annotations

from dataclasses import dataclass, field

from .models import JobRegistration, RecurringRegistration


@dataclass(slots=True)
class InMemoryDurableJobRegistry:
    """Registry implementation for local runtime registrations."""

    _jobs: dict[str, JobRegistration] = field(default_factory=dict)
    _recurring: dict[str, RecurringRegistration] = field(default_factory=dict)

    def register_job(self, registration: JobRegistration) -> None:
        if registration.name in self._jobs or registration.name in self._recurring:
            raise ValueError(f"job already registered: {registration.name}")
        self._jobs[registration.name] = registration

    def register_recurring(self, registration: RecurringRegistration) -> None:
        if registration.name in self._jobs or registration.name in self._recurring:
            raise ValueError(f"job already registered: {registration.name}")
        self._recurring[registration.name] = registration

    def get_job(self, name: str) -> JobRegistration | None:
        return self._jobs.get(name)

    def get_recurring(self, name: str) -> RecurringRegistration | None:
        return self._recurring.get(name)

    def get_all_recurring(self) -> list[RecurringRegistration]:
        return list(self._recurring.values())
