def map_zone(lat, lon, df_zones):
    for _, zone in df_zones.iterrows():
        if zone["tl_y"] >= lat >= zone["br_y"] and zone["tl_x"] <= lon <= zone["br_x"]:
            return zone["continent"]
    return None