from enricher import PostgresEnricher


class PostgresTransformer:
    def __init__(self, pg_enricher: PostgresEnricher):
        self.pg_enricher = pg_enricher

    def transform_data(self, rows):
        if not rows:
            return []
            
        movies_data = {}
        for row in rows:
            fw_id = str(row['fw_id'])
            if fw_id not in movies_data:
                movies_data[fw_id] = {
                    'id': fw_id,
                    'title': row['title'],
                    'description': row['description'],
                    'imdb_rating': row['rating'],
                    '_genres': set(),
                    '_actors': set(),
                    '_writers': set(),
                    '_directors': set(),
                }
            
            if row['genre_id']:
                movies_data[fw_id]['_genres'].add(row['genre_name'])

            if row['person_id']:
                person_tuple = (str(row['person_id']), row['full_name'])
                role = row['role']
                if role == 'actor':
                    movies_data[fw_id]['_actors'].add(person_tuple)
                elif role == 'writer':
                    movies_data[fw_id]['_writers'].add(person_tuple)
                elif role == 'director':
                    movies_data[fw_id]['_directors'].add(person_tuple)

        final_list = []
        for movie in movies_data.values():
            movie['genres'] = list(movie['_genres'])
            movie['actors'] = [{'id': pid, 'name': pname} for pid, pname in movie['_actors']]
            movie['writers'] = [{'id': pid, 'name': pname} for pid, pname in movie['_writers']]
            movie['directors'] = [{'id': pid, 'name': pname} for pid, pname in movie['_directors']]
            movie['actors_names'] = [pname for _, pname in movie['_actors']]
            movie['writers_names'] = [pname for _, pname in movie['_writers']]
            movie['directors_names'] = [pname for _, pname in movie['_directors']]

            del movie['_genres']
            del movie['_actors']
            del movie['_writers']
            del movie['_directors']
            
            final_list.append(movie)

        return final_list
