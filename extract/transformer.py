class Transformer:
    """
    Класс для трансформации плоских данных из БД в структурированные документы,
    соответствующие схеме Elasticsearch.
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
                    # Поля, которые напрямую соответствуют схеме
                    'id': fw_id,
                    'title': row['title'],
                    'description': row['description'],
                    'imdb_rating': row['rating'],
                    # Временные хранилища для дальнейшей агрегации
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

        # Финальная трансформация в формат, соответствующий схеме Elasticsearch
        final_list = []
        for movie in movies_data.values():
            # Преобразуем сеты в нужные структуры
            
            # 1. Жанры: плоский список имен
            movie['genres'] = list(movie['_genres'])

            # 2. Персоны: списки вложенных объектов (nested)
            movie['actors'] = [{'id': pid, 'name': pname} for pid, pname in movie['_actors']]
            movie['writers'] = [{'id': pid, 'name': pname} for pid, pname in movie['_writers']]
            movie['directors'] = [{'id': pid, 'name': pname} for pid, pname in movie['_directors']]

            # 3. Имена персон: плоские списки для полнотекстового поиска
            movie['actors_names'] = [pname for _, pname in movie['_actors']]
            movie['writers_names'] = [pname for _, pname in movie['_writers']]
            movie['directors_names'] = [pname for _, pname in movie['_directors']]

            # Удаляем временные поля, которых нет в схеме
            del movie['_genres']
            del movie['_actors']
            del movie['_writers']
            del movie['_directors']
            
            final_list.append(movie)

        return final_list
