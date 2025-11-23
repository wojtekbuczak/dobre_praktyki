from flask import Flask
from flask_restful import Resource, Api
from sqlalchemy import create_engine, Column, Integer, String, Float, TIMESTAMP, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
import pandas as pd
import os

# --- 1. Konfiguracja i Schemat Bazy Danych ---

# Definicja pliku bazy danych
DATABASE_FILE = 'movielens.db'
# String połączenia z bazą SQLite
DATABASE_URL = f"sqlite:///{DATABASE_FILE}"

# Utworzenie obiektu silnika SQLAlchemy
engine = create_engine(DATABASE_URL)

# Baza deklaratywna
Base = declarative_base()


# Definicja tabel w bazie danych
class Movie(Base):
    __tablename__ = 'movies'
    movieId = Column(Integer, primary_key=True)
    title = Column(String)
    genres = Column(String)

    # Definicja relacji (opcjonalne, ale dodaje spójności)
    links = relationship("Links", back_populates="movie", uselist=False)
    ratings = relationship("Ratings", back_populates="movie")
    tags = relationship("Tags", back_populates="movie")

    def to_dict(self):
        return {
            'movieId': self.movieId,
            'title': self.title,
            'genres': self.genres
        }


class Links(Base):
    __tablename__ = 'links'
    id = Column(Integer, primary_key=True)  # Dodany klucz główny
    movieId = Column(Integer, ForeignKey('movies.movieId'), unique=True)
    imdbId = Column(String)
    tmdbId = Column(String)

    movie = relationship("Movie", back_populates="links")

    def to_dict(self):
        return {
            'movieId': self.movieId,
            'imdbId': self.imdbId,
            'tmdbId': self.tmdbId
        }


class Ratings(Base):
    __tablename__ = 'ratings'
    id = Column(Integer, primary_key=True)  # Dodany klucz główny
    userId = Column(Integer)
    movieId = Column(Integer, ForeignKey('movies.movieId'))
    rating = Column(Float)
    timestamp = Column(Integer)

    movie = relationship("Movie", back_populates="ratings")

    def to_dict(self):
        return {
            'userId': self.userId,
            'movieId': self.movieId,
            'rating': self.rating,
            'timestamp': self.timestamp
        }


class Tags(Base):
    __tablename__ = 'tags'
    id = Column(Integer, primary_key=True)  # Dodany klucz główny
    userId = Column(Integer)
    movieId = Column(Integer, ForeignKey('movies.movieId'))
    tag = Column(String)
    timestamp = Column(Integer)

    movie = relationship("Movie", back_populates="tags")

    def to_dict(self):
        return {
            'userId': self.userId,
            'movieId': self.movieId,
            'tag': self.tag,
            'timestamp': self.timestamp
        }


# Utworzenie fabryki sesji
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# --- 2. Ładowanie Danych z CSV do Bazy ---

def initialize_database():
    """Tworzy tabele i ładuje dane z plików CSV, jeśli baza nie istnieje."""

    # Sprawdzenie, czy plik bazy danych istnieje
    if os.path.exists(DATABASE_FILE):
        print(f"Baza danych '{DATABASE_FILE}' już istnieje. Pomijam ładowanie danych.")
        return

    print("Tworzę tabele i ładuję dane z plików CSV...")

    # Tworzenie wszystkich tabel zdefiniowanych w Base
    Base.metadata.create_all(bind=engine)

    data_files = {
        'movies': 'movies.csv',
        'links': 'links.csv',
        'ratings': 'ratings.csv',
        'tags': 'tags.csv',
    }

    # Ładowanie danych za pomocą Pandas
    try:
        for table_name, file_name in data_files.items():
            # Wczytanie pliku CSV do DataFrame
            df = pd.read_csv(file_name)

            # Wpisanie danych do tabeli SQLite
            # if_exists='append' dodaje dane, index=False nie dodaje kolumny indeksu z DataFrame
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Załadowano dane do tabeli '{table_name}' z pliku '{file_name}'.")

    except FileNotFoundError as e:
        print(
            f"Błąd: Nie znaleziono pliku CSV: {e.filename}. Upewnij się, że pliki CSV znajdują się w tym samym katalogu.")
    except Exception as e:
        print(f"Wystąpił nieoczekiwany błąd podczas ładowania danych: {e}")


# --- 3. Refactor: Endpointy korzystające z Bazy Danych ---

# Inicjalizacja aplikacji Flask
app = Flask(__name__)
api = Api(app)


# Funkcja pomocnicza do pobierania sesji
def get_db():
    """Zwraca nową sesję SQLAlchemy."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Wprowadzenie klasy bazowej dla zasobów, aby zarządzać sesją
class BaseResource(Resource):
    """Klasa bazowa do wstrzykiwania sesji SQLAlchemy."""

    def get_session(self) -> Session:
        # Tworzy nową sesję i od razu ją zwraca (bez generatora)
        return SessionLocal()


# Klasa obsługująca endpoint /movies
class MoviesList(BaseResource):
    def get(self):
        with self.get_session() as db:
            # Użycie sesji do wykonania zapytania SELECT * FROM movies
            movies = db.query(Movie).all()
            # Konwersja listy obiektów Movie na listę słowników
            return [movie.to_dict() for movie in movies], 200


# Klasa obsługująca endpoint /links
class LinksList(BaseResource):
    def get(self):
        with self.get_session() as db:
            # Użycie sesji do wykonania zapytania SELECT * FROM links
            # W SQL: SELECT movieId, imdbId, tmdbId FROM links;
            links = db.query(Links.movieId, Links.imdbId, Links.tmdbId).all()

            # Konwersja listy krotek (wyników zapytania) na listę słowników
            return [
                {
                    'movieId': link[0],
                    'imdbId': link[1],
                    'tmdbId': link[2]
                } for link in links
            ], 200


# Klasa obsługująca endpoint /ratings
class RatingList(BaseResource):
    def get(self):
        with self.get_session() as db:
            # Użycie sesji do wykonania zapytania SELECT * FROM ratings
            ratings = db.query(Ratings).all()
            return [rating.to_dict() for rating in ratings], 200


# Klasa obsługująca endpoint /tags
class TagList(BaseResource):
    def get(self):
        with self.get_session() as db:
            # Użycie sesji do wykonania zapytania SELECT * FROM tags
            tags = db.query(Tags).all()
            return [tag.to_dict() for tag in tags], 200


# Rejestracja endpointów
api.add_resource(MoviesList, '/movies')
api.add_resource(LinksList, '/links')
api.add_resource(RatingList, '/ratings')
api.add_resource(TagList, '/tags')

if __name__ == '__main__':
    # Inicjalizacja bazy danych przed uruchomieniem aplikacji Flask
    initialize_database()
    app.run(debug=True)