from datetime import datetime

import pandas as pd
import psycopg2
import json
from sqlalchemy import create_engine
import time
import calendar
import datetime

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

SPOTIPY_CLIENT_ID="0a943ff50aa34b2c8feefd4e5744eade"
SPOTIPY_CLIENT_SECRET="1f5f68414e04427693a288a7143714ee"

artistas=["Michael Jackson", "Don Omar", "David Guetta", "Mago de Oz", "Jhon Alex Castaño"]


spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID,client_secret=SPOTIPY_CLIENT_SECRET))

data_df_db_artists = pd.DataFrame(columns=['name', 'popularity', 'type', 'uri', 'followers', 'origen', 'fecha_carga', 'fecha_carga_segundos'])
data_df_db_tracks = pd.DataFrame(columns=['name', 'type', 'artista', 'album_name', 'track_number', 'popularity', 'id', 'uri', 'lanzamiento', 'origen', 'fecha_carga', 'fecha_carga_segundos'])


for i in artistas:

    # Se hace la petición a spotify para traer el artista
    results = spotify.search(q='artist:' + i, type='artist', limit=1)
    items = results['artists']['items']
    artist = items[0]
    followers = artist['followers']['total']
    #print(followers)

    # Se almacenan los resultados en un dataframe utilizando pandas
    data_df = pd.DataFrame.from_dict(items)
    #print(data_df)

    data_df['followers'] = followers

    # Se agrega la fecha actual al dataframe
    fecha = datetime.datetime.now().timestamp()
    timestamp = datetime.datetime.fromtimestamp(fecha)
    data_df['fecha_carga'] = timestamp
    data_df['fecha_carga_segundos'] = fecha
    #print(data_df.dtypes)

    data_df['fecha_carga'] = pd.to_datetime(data_df['fecha_carga'])
    #data_df['fecha_carga'].dt.tz_localize(None)

    # Se agrega el origen al dataframe
    origen = data_df['external_urls'][0]['spotify']
    data_df['origen'] = origen
    #print(data_df.dtypes)

    #Se seleccionan los datos que van para la base de datos
    data_df_db_artists = data_df_db_artists.append(data_df[['name' , 'popularity' , 'type' , 'uri' , 'followers' , 'origen' , 'fecha_carga', 'fecha_carga_segundos']], ignore_index=True)
    #print(data_df_db)

    uri = data_df['uri']


    # Con la uri del artista buscamos el top 5 de tracks
    results2 = spotify.artist_top_tracks(uri[0])
    items2 = results2['tracks'][:5]
    #print(items2)
    data_df_tracks = pd.DataFrame.from_dict(items2)

    for j in items2:

        # se obtiene el artista del track
        artista_track = j['artists'][0]['name']
        data_df_tracks['artista'] = artista_track
        # print(artista_track)

        # Se obtiene el albun del track
        album_track = j['album']['name']
        data_df_tracks['album_name'] = album_track
        # print(album_track)

        # Se obtiene la fecha de lanzamiento del track
        fecha_lanzamiento = j['album']['release_date']
        data_df_tracks['lanzamiento'] = fecha_lanzamiento
        # print(fecha_lanzamiento)

        data_df_tracks['lanzamiento'] = pd.to_datetime(data_df_tracks['lanzamiento'], format='%Y-%m-%d')


        # Se carga la fecha de carga
        data_df_tracks['fecha_carga'] = timestamp
        data_df_tracks['fecha_carga'] = pd.to_datetime(data_df_tracks['fecha_carga'])

        data_df_tracks['fecha_carga_segundos'] = fecha

        # Se carga el origen
        origen_track = data_df_tracks['external_urls'][0]['spotify']
        data_df_tracks['origen'] = origen_track

        # Se seleccionan los datos que van para la base de datos
        data_df_db_tracks = data_df_db_tracks.append(data_df_tracks[
                                                         ['name', 'type', 'artista', 'album_name', 'track_number',
                                                          'popularity', 'id', 'uri', 'lanzamiento', 'origen',
                                                          'fecha_carga', 'fecha_carga_segundos']], ignore_index=True)



#print(data_df_db_artists.dtypes)
#print("---------------------------------")
#print(data_df_db_tracks.dtypes)
engine = create_engine('postgresql://postgres:12345@127.0.0.1:5432/spotify')
data_df_db_artists.to_sql('artista',con=engine,index=True)
data_df_db_tracks.to_sql('tracks',con=engine,index=True)
