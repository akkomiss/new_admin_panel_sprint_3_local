class Transformer:
    """
    Класс для трансформации плоских данных из БД в структурированные документы.
    """
    def transform_data(self, rows):
        """
        Принимает список строк из БД и аггрегирует их в документы фильмов.
        """
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
                    'type': row['type'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at'],
                    'genres': set(),
                    'actors': set(),
                    'writers': set(),
                    'directors': set(),
                }
            
            if row['genre_id']:
                movies_data[fw_id]['genres'].add(
                    (str(row['genre_id']), row['genre_name'])
                )

            if row['person_id']:
                person_tuple = (str(row['person_id']), row['full_name'])
                role = row['role']
                if role == 'actor':
                    movies_data[fw_id]['actors'].add(person_tuple)
                elif role == 'writer':
                    movies_data[fw_id]['writers'].add(person_tuple)
                elif role == 'director':
                    movies_data[fw_id]['directors'].add(person_tuple)

        final_list = []
        for movie in movies_data.values():
            movie['genres'] = [{'id': gid, 'name': gname} for gid, gname in movie['genres']]
            movie['actors'] = [{'id': pid, 'name': pname} for pid, pname in movie['actors']]
            movie['writers'] = [{'id': pid, 'name': pname} for pid, pname in movie['writers']]
            movie['directors'] = [{'id': pid, 'name': pname} for pid, pname in movie['directors']]
            final_list.append(movie)

        return final_list
