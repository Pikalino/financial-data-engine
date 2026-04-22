from app.database import Base, engine
from app import models  # noqa: F401


def init():
    Base.metadata.create_all(bind=engine)
    print("Database tables created.")


if __name__ == "__main__":
    init()