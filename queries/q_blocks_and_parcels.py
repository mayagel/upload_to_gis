

GET_ACTIVE_FROM_CENTRAL_CATALOG = """
            SELECT id, block_id, parcel_id, suffix_id, st_astext(polygon), active, 
                   catalog_update, catalog_insert, json_data
            FROM central_catalog_views.org_block_and_parcels_gis
            WHERE active = true
        """

Q_UPDT_BLOCKS_AND_PARCELS = """
        UPDATE {schema_name}.{table_name}
        SET
            block_id = :block_id,
            parcel_id = :parcel_id,
            suffix_id = :suffix_id,
            polygon = :polygon,
            active = :active,
            json_data = :json_data
        WHERE id = :id
    """ 

Q_INS_BLOCKS_AND_PARCELS = """
        INSERT INTO {schema_name}.{table_name} (
            id, block_id, parcel_id, suffix_id, polygon, active, json_data
        ) VALUES (
            :id, :block_id, :parcel_id, :suffix_id, :polygon, :active, :json_data
        )
    """ 