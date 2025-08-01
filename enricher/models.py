from dataclasses import dataclass
from typing import Optional
from datetime import datetime, date
from uuid import UUID

@dataclass
class film_work:
    id: UUID
    title: str
    description: Optional[str]
    creation_date: Optional[str]
    file_path: Optional[str]
    rating: Optional[float]
    type: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)

@dataclass
class genre:
    id: UUID
    name: Optional[str]
    description: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)

@dataclass
class person:
    id: UUID
    full_name: str
    created_at: Optional[str]
    updated_at: Optional[str]

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)

@dataclass
class genre_film_work:
    id: UUID
    genre_id: UUID
    film_work_id: UUID
    created_at: Optional[str]

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.genre_id, str):
            self.genre_id = UUID(self.genre_id)
        if isinstance(self.film_work_id, str):
            self.film_work_id = UUID(self.film_work_id)

@dataclass
class person_film_work:
    id: UUID
    person_id: UUID
    film_work_id: UUID
    role: str
    created_at: Optional[str]

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.person_id, str):
            self.person_id = UUID(self.person_id)
        if isinstance(self.film_work_id, str):
            self.film_work_id = UUID(self.film_work_id)