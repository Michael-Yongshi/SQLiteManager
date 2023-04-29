from dataclasses import dataclass

@dataclass
class Table:

    db: object
    name: str
    metadata: dict
    records: list