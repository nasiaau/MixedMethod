import pandas as pd
import numpy as np
import pyproj
from shapely.geometry import Point
from shapely.ops import transform
from typing import Optional
from tqdm import tqdm

import warnings
warnings.filterwarnings("ignore")


def generate_artificial_points(gdf_points, columns, a: int = 1, distance_meters: int = 100) -> None:
    n_points = (1 + 2 * a) ** 2
    correction = list(range(-a, a + 1))
    index = list(range(1, n_points + 1))

    proj_4326 = pyproj.Proj(init='epsg:4326')
    proj_3857 = pyproj.Proj(init='epsg:3857')
    transformer_to_3857 = pyproj.Transformer.from_proj(proj_4326, proj_3857, always_xy=True)
    transformer_to_4326 = pyproj.Transformer.from_proj(proj_3857, proj_4326, always_xy=True)

    df_points = pd.DataFrame(columns=['iterx', 'itery', 'lat', 'long', 'index']+columns)

    for _, init in tqdm(gdf_points.iterrows(), total=gdf_points.shape[0], desc='Processing initial points'):
        x_3857, y_3857 = transformer_to_3857.transform(init['x'], init['y'])
        
        tmpt = pd.DataFrame(columns=['iterx', 'itery', 'lat', 'long'])
        
        for x in range(1, (1 + 2 * a) + 1):
            for y in range(1, (1 + 2 * a) + 1):
                iterx = x
                itery = y
                
                new_x_3857 = x_3857 + correction[x - 1] * distance_meters
                new_y_3857 = y_3857 + correction[-y] * distance_meters
                
                new_long, new_lat = transformer_to_4326.transform(new_x_3857, new_y_3857)

                tmpt = pd.concat([tmpt, pd.DataFrame({'iterx': [iterx], 'itery': [itery], 'lat': [new_lat], 'long': [new_long]})], ignore_index=True)
        
        tmpt['index'] = index
        tmpt[columns] = init[columns]
        
        df_points = pd.concat([df_points, tmpt], ignore_index=True)
        
    return df_points