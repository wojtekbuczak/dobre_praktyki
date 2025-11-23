from flask import Flask, request
from flask_restful import Resource, Api, reqparse
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
import os
from sqlalchemy.orm.exc import NoResultFound
from contextlib import contextmanager
import json

# --- 1. Konfiguracja i Schemat Bazy Danych ---

TESTING_MODE = os.environ.get('FLASK_ENV') == 'testing'
DATABASE_FILE = 'movielens.db'

# Inicjalizacja URL-a
if TESTING_MODE:
    # W trybie testowym używamy UNIKALNEGO URL-a dla każdego testu (w ramach engine)
    # W Pytest, engine jest tworzone i niszczone dla każdej fixture,
    # ale musimy upewnić się, że nie ładujemy domyślnego engine z pliku, jeśli już istnieje.
    DATABASE_URL_RUNTIME = "sqlite:///:memory:"
else:
    DATABASE_URL_RUNTIME = f"sqlite:///{DATABASE_FILE}"

# Ustawienie globalnych zmiennych na None
engine = None
SessionLocal = None
Base = declarative_base()


# Definicja tabel w bazie danych (bez zmian)
class Movie(Base):
    __tablename__ = 'movies'
    movieId = Column(Integer, primary_key=True, autoincrement=False)
    title = Column(String)
    genres = Column(String)

    links = relationship("Links", back_populates="movie", uselist=False, cascade="all, delete-orphan")
    ratings = relationship("Ratings", back_populates="movie", cascade="all, delete-orphan")
    tags = relationship("Tags", back_populates="movie", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            'movieId': self.movieId,
            'title': self.title,
            'genres': self.genres
        }


class Links(Base):
    __tablename__ = 'links'
    id = Column(Integer, primary_key=True)
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
    id = Column(Integer, primary_key=True)
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
    id = Column(Integer, primary_key=True)
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


# --- 2. Zarządzanie Bazą Danych i Sesjami ---

def configure_db(db_url):
    """Tworzy i konfiguruje globalny silnik oraz fabrykę sesji."""
    global engine, SessionLocal
    engine = create_engine(db_url)

    # Używamy Base.metadata.create_all dla in-memory bazy i nowej bazy plikowej
    Base.metadata.create_all(bind=engine)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine


@contextmanager
def get_db():
    """Context Manager dla sesji SQLAlchemy."""
    if SessionLocal is None:
        # Ten wyjątek powinien być obsłużony przez create_app/initialize_database
        raise Exception("Database not configured. Call initialize_database() or create_app() first.")

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def initialize_database():
    """Konfiguruje bazę plikową i tworzy tabele, jeśli plik nie istnieje."""
    if os.path.exists(DATABASE_FILE) and not TESTING_MODE:
        print(f"Baza danych '{DATABASE_FILE}' już istnieje. Pomijam tworzenie schematu.")
        configure_db(DATABASE_URL_RUNTIME)
        return

    if not TESTING_MODE:
        print("Tworzę schemat bazy danych (tabele)...")
        configure_db(DATABASE_URL_RUNTIME)
        print(f"Utworzono pustą bazę danych '{DATABASE_FILE}'.")


# --- 3. Endpointy korzystające z Bazy Danych ---

def create_app(test_config=None):
    global engine, SessionLocal
    app = Flask(__name__)
    api = Api(app)

    if test_config:
        app.config.update(test_config)

    # KLUCZOWA POPRAWKA: Jeśli engine nie jest zainicjowane, zrób to teraz
    if engine is None:
        # W trybie testowym DATABASE_URL_RUNTIME to :memory:
        configure_db(DATABASE_URL_RUNTIME)

        # Klasa bazowa do wstrzykiwania sesji (poprawiona)

    class BaseResource(Resource):
        def get_session(self):
            return get_db()

    # ... (implementacja endpointów LIST i ITEM pozostaje bez zmian) ...
    # Zostawiamy pełne definicje, aby plik był kompletny:

    # --- Parsery Argumentów ---

    movie_parser = reqparse.RequestParser()
    movie_parser.add_argument('movieId', type=int, required=True, help='Pole MovieId jest wymagane', location='json')
    movie_parser.add_argument('title', type=str, required=True, help='Pole Title jest wymagane', location='json')
    movie_parser.add_argument('genres', type=str, required=True, help='Pole Genres jest wymagane', location='json')

    links_parser = reqparse.RequestParser()
    links_parser.add_argument('movieId', type=int, required=True, help='Pole MovieId jest wymagane', location='json')
    links_parser.add_argument('imdbId', type=str, required=True, help='Pole ImdbId jest wymagane', location='json')
    links_parser.add_argument('tmdbId', type=str, required=True, help='Pole TmdbId jest wymagane', location='json')

    ratings_parser = reqparse.RequestParser()
    ratings_parser.add_argument('userId', type=int, required=True, help='Pole UserId jest wymagane', location='json')
    ratings_parser.add_argument('movieId', type=int, required=True, help='Pole MovieId jest wymagane', location='json')
    ratings_parser.add_argument('rating', type=float, required=True, help='Pole Rating jest wymagane', location='json')
    ratings_parser.add_argument('timestamp', type=int, required=True, help='Pole Timestamp jest wymagane',
                                location='json')

    tags_parser = reqparse.RequestParser()
    tags_parser.add_argument('userId', type=int, required=True, help='Pole UserId jest wymagane', location='json')
    tags_parser.add_argument('movieId', type=int, required=True, help='Pole MovieId jest wymagane', location='json')
    tags_parser.add_argument('tag', type=str, required=True, help='Pole Tag jest wymagane', location='json')
    tags_parser.add_argument('timestamp', type=int, required=True, help='Pole Timestamp jest wymagane', location='json')

    # --- Endpointy List (GET, POST) ---

    class MoviesList(BaseResource):
        def get(self):
            with self.get_session() as db:
                movies = db.query(Movie).all()
                return [movie.to_dict() for movie in movies], 200

        def post(self):
            args = movie_parser.parse_args()
            new_movie = Movie(
                movieId=args['movieId'],
                title=args['title'],
                genres=args['genres']
            )
            with self.get_session() as db:
                if db.query(Movie).filter(Movie.movieId == args['movieId']).first():
                    return {'message': f'Film o ID {args["movieId"]} już istnieje.'}, 409
                db.add(new_movie)
                db.commit()
                return new_movie.to_dict(), 201

    class LinksList(BaseResource):
        def get(self):
            with self.get_session() as db:
                links = db.query(Links.movieId, Links.imdbId, Links.tmdbId).all()
                return [
                    {
                        'movieId': link[0],
                        'imdbId': link[1],
                        'tmdbId': link[2]
                    } for link in links
                ], 200

        def post(self):
            args = links_parser.parse_args()
            new_link = Links(
                movieId=args['movieId'],
                imdbId=args['imdbId'],
                tmdbId=args['tmdbId']
            )
            with self.get_session() as db:
                if db.query(Links).filter(Links.movieId == args['movieId']).first():
                    return {'message': f'Link dla filmu o ID {args["movieId"]} już istnieje.'}, 409

                if not db.query(Movie).filter(Movie.movieId == args['movieId']).first():
                    return {'message': f'Film o ID {args["movieId"]} nie istnieje (Foreign Key Error).'}, 400

                db.add(new_link)
                db.commit()
                return new_link.to_dict(), 201

    class RatingList(BaseResource):
        def get(self):
            with self.get_session() as db:
                ratings = db.query(Ratings).all()
                return [rating.to_dict() for rating in ratings], 200

        def post(self):
            args = ratings_parser.parse_args()
            new_rating = Ratings(
                userId=args['userId'],
                movieId=args['movieId'],
                rating=args['rating'],
                timestamp=args['timestamp']
            )
            with self.get_session() as db:
                if not db.query(Movie).filter(Movie.movieId == args['movieId']).first():
                    return {'message': f'Film o ID {args["movieId"]} nie istnieje (Foreign Key Error).'}, 400

                db.add(new_rating)
                db.commit()
                return new_rating.to_dict(), 201

    class TagList(BaseResource):
        def get(self):
            with self.get_session() as db:
                tags = db.query(Tags).all()
                return [tag.to_dict() for tag in tags], 200

        def post(self):
            args = tags_parser.parse_args()
            new_tag = Tags(
                userId=args['userId'],
                movieId=args['movieId'],
                tag=args['tag'],
                timestamp=args['timestamp']
            )
            with self.get_session() as db:
                if not db.query(Movie).filter(Movie.movieId == args['movieId']).first():
                    return {'message': f'Film o ID {args["movieId"]} nie istnieje (Foreign Key Error).'}, 400

                db.add(new_tag)
                db.commit()
                return new_tag.to_dict(), 201

    # --- Endpointy dla Pojedynczych Elementów (GET, PUT, DELETE) ---

    class MovieItem(BaseResource):
        def get(self, movie_id):
            with self.get_session() as db:
                try:
                    movie = db.query(Movie).filter(Movie.movieId == movie_id).one()
                    return movie.to_dict(), 200
                except NoResultFound:
                    return {'message': f'Film o ID {movie_id} nie znaleziony.'}, 404

        def put(self, movie_id):
            args = movie_parser.parse_args()
            with self.get_session() as db:
                try:
                    movie = db.query(Movie).filter(Movie.movieId == movie_id).one()
                    movie.title = args['title']
                    movie.genres = args['genres']
                    db.commit()
                    return movie.to_dict(), 200
                except NoResultFound:
                    return {'message': f'Film o ID {movie_id} nie znaleziony.'}, 404

        def delete(self, movie_id):
            with self.get_session() as db:
                try:
                    movie = db.query(Movie).filter(Movie.movieId == movie_id).one()
                    db.delete(movie)
                    db.commit()
                    return {'message': f'Film o ID {movie_id} usunięty.'}, 204
                except NoResultFound:
                    return {'message': f'Film o ID {movie_id} nie znaleziony.'}, 404

    class LinksItem(BaseResource):
        def get(self, movie_id):
            with self.get_session() as db:
                try:
                    link = db.query(Links).filter(Links.movieId == movie_id).one()
                    return link.to_dict(), 200
                except NoResultFound:
                    return {'message': f'Link dla filmu o ID {movie_id} nie znaleziony.'}, 404

        def put(self, movie_id):
            args = links_parser.parse_args()
            with self.get_session() as db:
                try:
                    link = db.query(Links).filter(Links.movieId == movie_id).one()
                    link.imdbId = args['imdbId']
                    link.tmdbId = args['tmdbId']
                    db.commit()
                    return link.to_dict(), 200
                except NoResultFound:
                    return {'message': f'Link dla filmu o ID {movie_id} nie znaleziony.'}, 404

        def delete(self, movie_id):
            with self.get_session() as db:
                try:
                    link = db.query(Links).filter(Links.movieId == movie_id).one()
                    db.delete(link)
                    db.commit()
                    return {'message': f'Link dla filmu o ID {movie_id} usunięty.'}, 204
                except NoResultFound:
                    return {'message': f'Link dla filmu o ID {movie_id} nie znaleziony.'}, 404

    class RatingItem(BaseResource):
        def get(self, rating_id):
            with self.get_session() as db:
                try:
                    rating = db.query(Ratings).filter(Ratings.id == rating_id).one()
                    return rating.to_dict(), 200
                except NoResultFound:
                    return {'message': f'Ocena o ID {rating_id} nie znaleziona.'}, 404

        def put(self, rating_id):
            args = ratings_parser.parse_args()
            with self.get_session() as db:
                try:
                    rating = db.query(Ratings).filter(Ratings.id == rating_id).one()
                    rating.userId = args['userId']
                    rating.movieId = args['movieId']
                    rating.rating = args['rating']
                    rating.timestamp = args['timestamp']
                    db.commit()
                    return rating.to_dict(), 200
                except NoResultFound:
                    return {'message': f'Ocena o ID {rating_id} nie znaleziona.'}, 404

        def delete(self, rating_id):
            with self.get_session() as db:
                try:
                    rating = db.query(Ratings).filter(Ratings.id == rating_id).one()
                    db.delete(rating)
                    db.commit()
                    return {'message': f'Ocena o ID {rating_id} usunięta.'}, 204
                except NoResultFound:
                    return {'message': f'Ocena o ID {rating_id} nie znaleziona.'}, 404

    class TagItem(BaseResource):
        def get(self, tag_id):
            with self.get_session() as db:
                try:
                    tag = db.query(Tags).filter(Tags.id == tag_id).one()
                    return tag.to_dict(), 200
                except NoResultFound:
                    return {'message': f'Tag o ID {tag_id} nie znaleziony.'}, 404

        def put(self, tag_id):
            args = tags_parser.parse_args()
            with self.get_session() as db:
                try:
                    tag = db.query(Tags).filter(Tags.id == tag_id).one()
                    tag.userId = args['userId']
                    tag.movieId = args['movieId']
                    tag.tag = args['tag']
                    tag.timestamp = args['timestamp']
                    db.commit()
                    return tag.to_dict(), 200
                except NoResultFound:
                    return {'message': f'Tag o ID {tag_id} nie znaleziony.'}, 404

        def delete(self, tag_id):
            with self.get_session() as db:
                try:
                    tag = db.query(Tags).filter(Tags.id == tag_id).one()
                    db.delete(tag)
                    db.commit()
                    return {'message': f'Tag o ID {tag_id} usunięty.'}, 204
                except NoResultFound:
                    return {'message': f'Tag o ID {tag_id} nie znaleziony.'}, 404

    # Rejestracja endpointów
    api.add_resource(MoviesList, '/movies')
    api.add_resource(LinksList, '/links')
    api.add_resource(RatingList, '/ratings')
    api.add_resource(TagList, '/tags')

    api.add_resource(MovieItem, '/movies/<int:movie_id>')
    api.add_resource(LinksItem, '/links/<int:movie_id>')
    api.add_resource(RatingItem, '/ratings/<int:rating_id>')
    api.add_resource(TagItem, '/tags/<int:tag_id>')

    return app


# Inicjalizacja poza trybem testowym
if not TESTING_MODE and __name__ == '__main__':
    initialize_database()
    app = create_app()
    app.run(debug=True)