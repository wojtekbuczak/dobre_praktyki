from flask import Flask
from flask_restful import Resource, Api
import csv

# Inicjalizacja aplikacji Flask
app = Flask(__name__)
api = Api(app)

class Movie:
    def __init__(self, movieId, title, genres):
        self.movieId = movieId
        self.title = title
        self.genres = genres

    def __repr__(self):
        return f"<Movie {self.movieId}>"

class Links:
    def __init__(self, movieId, imdbId, tmdbId):
        self.movieId = movieId
        self.imdbId = imdbId
        self.tmdbId = tmdbId

    def __repr__(self):
        return f"<Links {self.movieId}>"

class Ratings:
    def __init__(self, userId, movieId, rating, timestamp):
        self.userId = userId
        self.movieId = movieId
        self.rating = rating
        self.timestamp = timestamp

    def __repr__(self):
        return f"<Ratings {self.userId}>"

class Tags:
    def __init__(self, userId, movieId, tag, timestamp):
        self.userId = userId
        self.movieId = movieId
        self.tag = tag
        self.timestamp = timestamp

    def __repr__(self):
        return f"<Tags {self.userId}>"

# Klasa obsługująca endpoint /movies
class MoviesList(Resource):
    def get(self):
        movies = []

        # Pobranie danych z pliku CSV
        try:
            with open('movies.csv', 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Tworzenie obiektu Movie na podstawie wiersza
                    movie = Movie(row['movieId'], row['title'], row['genres'])
                    # Serializacja obiektu przy użyciu __dict__
                    movies.append(movie.__dict__)
        except FileNotFoundError:
            return {"error": "File not found"}, 404

        # Zwracanie listy zserializowanych obiektów
        return movies, 200

# Klasa obsługująca endpoint /links
class LinksList(Resource):
    def get(self):
        links = []

        # Pobranie danych z pliku CSV
        try:
            with open('links.csv', 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    link = Links(row['movieId'], row['imdbId'], row['tmdbId'])
                    links.append(link.__dict__)
        except FileNotFoundError:
            return {"error": "File not found"}, 404

        # Zwracanie listy zserializowanych obiektów
        return links, 200

# Klasa obsługująca endpoint /ratings
class RatingList(Resource):
    def get(self):
        ratings = []

        # Pobranie danych z pliku CSV
        try:
            with open('ratings.csv', 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    rating = Ratings(row['userId'], row['movieId'], row['rating'], row['timestamp'])
                    ratings.append(rating.__dict__)
        except FileNotFoundError:
            return {"error": "File not found"}, 404

        # Zwracanie listy zserializowanych obiektów
        return ratings, 200

# Klasa obsługująca endpoint /tags
class TagList(Resource):
    def get(self):
        tags = []

        # Pobranie danych z pliku CSV
        try:
            with open('tags.csv', 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    tag = Tags(row['userId'], row['movieId'], row['tag'], row['timestamp'])
                    tags.append(tag.__dict__)
        except FileNotFoundError:
            return {"error": "File not found"}, 404

        # Zwracanie listy zserializowanych obiektów
        return tags, 200

# Rejestracja endpointów
api.add_resource(MoviesList, '/movies')
api.add_resource(LinksList, '/links')
api.add_resource(RatingList, '/ratings')
api.add_resource(TagList, '/tags')

if __name__ == '__main__':
    app.run(debug=True)