import pytest
import os
import json
from sqlalchemy.orm import sessionmaker
# Importujemy konfigurację i modele z app.py
from app import create_app, Base, get_db, Movie, Links, Ratings, Tags, configure_db

# Ustawienie zmiennej środowiskowej, aby wymusić tryb testowy (in-memory database)
# To musi być ustawione PRZED załadowaniem aplikacji.
os.environ['FLASK_ENV'] = 'testing'

# --- Stałe dla danych testowych ---
TEST_MOVIE_ID = 999999
TEST_MOVIE_POST_ID = 1000000

# Zmienne globalne, w których przechowamy ID wygenerowane przez bazę danych
# Będą one ustawione przez fixture setup_global_ids
GENERATED_RATING_ID = None
GENERATED_TAG_ID = None

# Inicjalizacja modeli testowych. Nie podajemy 'id' dla Links, Ratings, Tags,
# aby SQLite mógł je automatycznie wygenerować.
TEST_FIXTURES = {
    'movie': Movie(movieId=TEST_MOVIE_ID, title="Test Movie", genres="Action|Test"),
    'link': Links(movieId=TEST_MOVIE_ID, imdbId="9999999", tmdbId="999999"),
    'rating': Ratings(userId=1, movieId=TEST_MOVIE_ID, rating=5.0, timestamp=1609459200),
    'tag': Tags(userId=1, movieId=TEST_MOVIE_ID, tag="test", timestamp=1609459200),
}


@pytest.fixture(scope="module")
def app():
    """
    Fixture tworząca instancję aplikacji testowej i konfigurująca czystą bazę in-memory.
    Zakres 'module' oznacza, że to samo app i engine będą używane dla wszystkich testów w tym module.
    """
    # W trybie 'testing', create_app wywoła configure_db(":memory:")
    app_instance = create_app({'TESTING': True})

    yield app_instance

    # Czyszczenie: Baza in-memory zostanie zamknięta wraz z procesem Pytest.
    pass


@pytest.fixture(scope="module")
def setup_global_ids(app):
    """
    Fixture wstrzykująca dane testowe do CZYSTEJ bazy i przechwytująca wygenerowane ID.
    Uruchamiana raz dla modułu.
    """
    global GENERATED_RATING_ID, GENERATED_TAG_ID

    # Wstrzyknięcie testowych danych do bazy (używamy tej samej bazy, co app)
    with get_db() as db:
        # Wstawienie filmu (dla klucza obcego)
        db.add(TEST_FIXTURES['movie'])
        db.commit()

        # Wstawienie pozostałych elementów (Links musi odwoływać się do istniejącego Movie)
        db.add(TEST_FIXTURES['link'])
        db.add(TEST_FIXTURES['rating'])
        db.add(TEST_FIXTURES['tag'])

        db.commit()

        # Przechwyć automatycznie wygenerowane ID
        GENERATED_RATING_ID = TEST_FIXTURES['rating'].id
        GENERATED_TAG_ID = TEST_FIXTURES['tag'].id

    # Zwraca cokolwiek, Pytest użyje tego do porządkowania kolejności
    return True


@pytest.fixture
def client(app, setup_global_ids):
    """Fixture zwracająca klienta testowego Flask. Zależy od setup_global_ids."""
    return app.test_client()


# ==========================================================
# --- TESTY DLA ZASOBÓW LIST (GET ALL, POST) ---
# ==========================================================

def test_movies_list_get(client):
    """Weryfikacja, czy lista zawiera tylko wstrzyknięty element testowy."""
    response = client.get('/movies')
    data = response.get_json()
    assert response.status_code == 200
    # Oczekujemy 1, ponieważ baza jest CZYSTA i zawiera tylko wstrzyknięty 'Test Movie'
    assert len(data) == 1
    assert data[0]['title'] == 'Test Movie'


def test_movies_list_post(client):
    """Weryfikacja dodawania nowego filmu do bazy."""
    new_movie_id = TEST_MOVIE_POST_ID
    new_movie_data = {
        'movieId': new_movie_id,
        'title': 'New Movie Post',
        'genres': 'Comedy|Test'
    }
    response = client.post('/movies', json=new_movie_data)
    data = response.get_json()

    assert response.status_code == 201
    assert data['movieId'] == new_movie_id

    # Sprawdzenie w bazie (w celu potwierdzenia trwałego zapisu)
    with get_db() as db:
        new_movie = db.query(Movie).filter(Movie.movieId == new_movie_id).first()
        assert new_movie is not None


# ==========================================================
# --- TESTY DLA ZASOBÓW ITEM (GET ONE, PUT, DELETE) ---
# ==========================================================

# --- Movies ---

def test_movies_item_get_found(client):
    """Weryfikacja pobierania pojedynczego zasobu, który istnieje."""
    response = client.get(f'/movies/{TEST_MOVIE_ID}')
    data = response.get_json()
    assert response.status_code == 200
    assert data['movieId'] == TEST_MOVIE_ID


def test_movies_item_get_not_found(client):
    """Weryfikacja statusu 404 dla nieistniejącego ID."""
    response = client.get('/movies/99999999999')
    assert response.status_code == 404


def test_movies_item_put(client):
    """Weryfikacja aktualizacji istniejącego zasobu."""
    updated_title = 'Updated Test Movie'
    updated_data = {
        # movieId musi być w payloadzie ze względu na parser, ale jest ignorowane w PUT w logice
        'movieId': TEST_MOVIE_ID,
        'title': updated_title,
        'genres': 'Drama|Updated'
    }
    response = client.put(f'/movies/{TEST_MOVIE_ID}', json=updated_data)
    assert response.status_code == 200

    with get_db() as db:
        updated_movie = db.query(Movie).filter(Movie.movieId == TEST_MOVIE_ID).one()
        assert updated_movie.title == updated_title


def test_movies_item_delete(client):
    """Weryfikacja usuwania istniejącego zasobu (tworzymy go przed testem)."""
    movie_to_delete_id = 999998
    with get_db() as db:
        db.add(Movie(movieId=movie_to_delete_id, title="Temp Delete", genres="Test"))
        db.commit()

    response = client.delete(f'/movies/{movie_to_delete_id}')
    assert response.status_code == 204

    with get_db() as db:
        deleted_movie = db.query(Movie).filter(Movie.movieId == movie_to_delete_id).first()
        assert deleted_movie is None


# --- Links ---
# Links używają MovieId jako unikalnego ID dla endpointu item

def test_links_item_put(client):
    """Weryfikacja aktualizacji istniejącego linku."""
    updated_imdb = '1234567'
    updated_data = {
        'movieId': TEST_MOVIE_ID,
        'imdbId': updated_imdb,
        'tmdbId': '987654'
    }
    response = client.put(f'/links/{TEST_MOVIE_ID}', json=updated_data)
    assert response.status_code == 200

    with get_db() as db:
        updated_link = db.query(Links).filter(Links.movieId == TEST_MOVIE_ID).one()
        assert updated_link.imdbId == updated_imdb


def test_links_item_delete(client):
    """Weryfikacja usuwania istniejącego linku (tworzymy go przed testem)."""
    temp_movie_id = 999997
    with get_db() as db:
        db.add(Movie(movieId=temp_movie_id, title="Temp Link Delete", genres="Test"))
        db.add(Links(movieId=temp_movie_id, imdbId="111", tmdbId="222"))
        db.commit()

    response = client.delete(f'/links/{temp_movie_id}')
    assert response.status_code == 204

    with get_db() as db:
        deleted_link = db.query(Links).filter(Links.movieId == temp_movie_id).first()
        assert deleted_link is None


# --- Ratings ---
# Ratings używają GENERATED_RATING_ID jako klucza głównego

def test_ratings_item_put(client):
    """Weryfikacja aktualizacji istniejącej oceny przy użyciu wygenerowanego ID."""
    global GENERATED_RATING_ID
    assert GENERATED_RATING_ID is not None

    updated_rating = 1.0
    updated_data = {
        'userId': 1,
        'movieId': TEST_MOVIE_ID,
        'rating': updated_rating,
        'timestamp': 1609459201
    }
    response = client.put(f'/ratings/{GENERATED_RATING_ID}', json=updated_data)
    assert response.status_code == 200

    with get_db() as db:
        updated_rating_db = db.query(Ratings).filter(Ratings.id == GENERATED_RATING_ID).one()
        assert updated_rating_db.rating == updated_rating


def test_ratings_item_delete(client):
    """Weryfikacja usuwania istniejącej oceny (tworzymy ją przed testem)."""
    temp_rating_id = 888886
    with get_db() as db:
        # Uwaga: Musimy jawnie ustawić ID dla tego rekordu, bo inaczej SQLAlchemy użyje
        # licznika, który może być już wysoki.
        db.add(Ratings(id=temp_rating_id, userId=1, movieId=TEST_MOVIE_ID, rating=4.0, timestamp=1))
        db.commit()

    response = client.delete(f'/ratings/{temp_rating_id}')
    assert response.status_code == 204

    with get_db() as db:
        deleted_rating = db.query(Ratings).filter(Ratings.id == temp_rating_id).first()
        assert deleted_rating is None


# --- Tags ---
# Tags używają GENERATED_TAG_ID jako klucza głównego

def test_tags_item_put(client):
    """Weryfikacja aktualizacji istniejącego tagu przy użyciu wygenerowanego ID."""
    global GENERATED_TAG_ID
    assert GENERATED_TAG_ID is not None

    updated_tag = 'updated_test'
    updated_data = {
        'userId': 1,
        'movieId': TEST_MOVIE_ID,
        'tag': updated_tag,
        'timestamp': 1609459201
    }
    response = client.put(f'/tags/{GENERATED_TAG_ID}', json=updated_data)
    assert response.status_code == 200

    with get_db() as db:
        updated_tag_db = db.query(Tags).filter(Tags.id == GENERATED_TAG_ID).one()
        assert updated_tag_db.tag == updated_tag


def test_tags_item_delete(client):
    """Weryfikacja usuwania istniejącego tagu (tworzymy go przed testem)."""
    temp_tag_id = 777776
    with get_db() as db:
        db.add(Tags(id=temp_tag_id, userId=1, movieId=TEST_MOVIE_ID, tag="Temp Delete", timestamp=1))
        db.commit()

    response = client.delete(f'/tags/{temp_tag_id}')
    assert response.status_code == 204

    with get_db() as db:
        deleted_tag = db.query(Tags).filter(Tags.id == temp_tag_id).first()
        assert deleted_tag is None