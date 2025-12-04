# author: yagel maimon
# time: 04/12/2025 09:20
# description: script to get blocks_and_parcels table from central-catalog and upload to gis postgres
# steps:
# 1. Connect to central-catalog database
# 2. Query blocks_and_parcels table
# 3. process data and create temp gdb
# 4. Upload data to GIS postgres

import sys
import os
import json
import logging
from datetime import datetime

imports_dirname = os.path.dirname(__file__)
main_dirname = os.path.dirname(imports_dirname)
sys.path.append(main_dirname)

# mess with DB
from pathlib import Path
from dataclasses import dataclass
from datetime import date
from tqdm import tqdm

import arcpy

from common.db_operations import DatabaseOperations 
from queries.q_blocks_and_parcels import GET_ACTIVE_FROM_CENTRAL_CATALOG

from common.config import postgres_SDE_path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def map_json_to_gdb_columns(json_data):
    """
    Map JSON data keys to GDB column names.
    
    Args:
        json_data: Dictionary with JSON data from database
        
    Returns:
        Dictionary with mapped values for GDB columns
    """
    # Mapping from JSON keys to GDB column names
    mapping = {
        'PARCEL_ID': 'parcel_id',
        'GUSH_NUM': 'gush_num',
        'GUSH_SUFFI': 'gush_suffix',
        'PARCEL': 'parcel',
        'LEGAL_AREA': 'legal_area',
        'STATUS': 'status',
        'STATUS_TEX': 'status_text',
        'LOCALITY_I': 'locality_id',
        'LOCALITY_N': 'locality_name',
        'REG_MUN_ID': 'reg_mun_id',
        'REG_MUN_NA': 'reg_mun_name',
        'COUNTY_ID': 'county_id',
        'COUNTY_NAM': 'county_name',
        'REGION_ID': 'region_id',
        'REGION_NAM': 'region_name',
        'TALAR_NUMB': 'talar_numb',
        'TALAR_YEAR': 'talar_year',
        'SYS_DATE': 'idkun_talar_date'  # Special case: SYS_DATE -> idkun_talar_date
    }
    
    # All GDB columns (excluding shape which is handled separately)
    gdb_columns = [
        'parcel_id', 'gush_num', 'gush_suffix', 'parcel', 'pnumtype', 'pnumtype_text',
        'legal_area', 'status', 'status_text', 'locality_id', 'locality_name',
        'reg_mun_id', 'reg_mun_name', 'county_id', 'county_name', 'region_id',
        'region_name', 'wp', 'wp_status', 'wp_status_text', 'talar_numb',
        'talar_year', 'idkun_talar_date', 'xoid', 'gparcel', 'globalid',
        'gdb_archive_oid', 'gdb_from_date', 'gdb_to_date'
    ]
    
    # Initialize result dictionary with None values
    result = {col: None for col in gdb_columns}
    
    # Map values from JSON data
    for json_key, gdb_col in mapping.items():
        if json_key in json_data:
            value = json_data[json_key]
            
            result[gdb_col] = value
    
    return result


def wkt_to_arcpy_geometry(wkt_string, spatial_reference=None):
    """
    Convert WKT string to arcpy geometry object.
    
    Args:
        wkt_string: WKT formatted polygon string
        spatial_reference: Optional spatial reference (defaults to EPSG:2039)
        
    Returns:
        arcpy.Polygon object
    """
    if not wkt_string:
        return None
    
    try:
        # Set default spatial reference if not provided
        if spatial_reference is None:
            spatial_reference = arcpy.SpatialReference(2039)  # EPSG:2039
        
        # Use arcpy.FromWKT to convert WKT to geometry
        # Note: FromWKT is available in ArcGIS Pro 2.1+
        geometry = arcpy.FromWKT(wkt_string, spatial_reference)
        return geometry
    except AttributeError:
        # Fallback for older arcpy versions - try using AsShape with GeoJSON
        try:
            # Convert WKT to GeoJSON format (simplified approach)
            # This is a fallback - may need shapely for proper conversion
            logger.warning("arcpy.FromWKT not available, trying alternative method")
            # For now, return None and log error
            logger.error("WKT conversion requires arcpy.FromWKT (ArcGIS Pro 2.1+)")
            return None
        except Exception as e:
            logger.error(f"Error in fallback WKT conversion: {e}")
            return None
    except Exception as e:
        logger.error(f"Error converting WKT to geometry: {e}")
        if wkt_string:
            logger.error(f"WKT string (first 200 chars): {wkt_string[:200]}...")
        return None


def create_gdb_and_feature_class(output_dir, gdb_name, feature_class_name):
    """
    Create a GDB file and polygon feature class with required columns.
    
    Args:
        output_dir: Directory to create GDB in
        gdb_name: Name of the GDB file (without .gdb extension)
        feature_class_name: Name of the feature class
        
    Returns:
        Path to the created feature class
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    gdb_path = os.path.join(output_dir, f"{gdb_name}.gdb")
    
    # Delete existing GDB if it exists
    if arcpy.Exists(gdb_path):
        logger.info(f"Deleting existing GDB: {gdb_path}")
        arcpy.Delete_management(gdb_path)
    
    # Create GDB
    logger.info(f"Creating GDB: {gdb_path}")
    arcpy.CreateFileGDB_management(output_dir, f"{gdb_name}.gdb")
    
    # Define spatial reference (using EPSG:2039)
    spatial_ref = arcpy.SpatialReference(2039)  # EPSG:2039
    
    # Define field mappings for the feature class
    # Field name, field type, field length, field precision, field scale, field alias, field nullable
    fields = [
        ('parcel_id', 'LONG', None, None, None, 'Parcel ID', 'NULLABLE'),
        ('gush_num', 'LONG', None, None, None, 'Gush Number', 'NULLABLE'),
        ('gush_suffix', 'LONG', None, None, None, 'Gush Suffix', 'NULLABLE'),
        ('parcel', 'LONG', None, None, None, 'Parcel', 'NULLABLE'),
        ('pnumtype', 'SHORT', None, None, None, 'Pnum Type', 'NULLABLE'),
        ('pnumtype_text', 'TEXT', 50, None, None, 'Pnum Type Text', 'NULLABLE'),
        ('legal_area', 'DOUBLE', None, None, None, 'Legal Area', 'NULLABLE'),
        ('status', 'SHORT', None, None, None, 'Status', 'NULLABLE'),
        ('status_text', 'TEXT', 100, None, None, 'Status Text', 'NULLABLE'),
        ('locality_id', 'LONG', None, None, None, 'Locality ID', 'NULLABLE'),
        ('locality_name', 'TEXT', 100, None, None, 'Locality Name', 'NULLABLE'),
        ('reg_mun_id', 'LONG', None, None, None, 'Reg Mun ID', 'NULLABLE'),
        ('reg_mun_name', 'TEXT', 100, None, None, 'Reg Mun Name', 'NULLABLE'),
        ('county_id', 'LONG', None, None, None, 'County ID', 'NULLABLE'),
        ('county_name', 'TEXT', 100, None, None, 'County Name', 'NULLABLE'),
        ('region_id', 'LONG', None, None, None, 'Region ID', 'NULLABLE'),
        ('region_name', 'TEXT', 100, None, None, 'Region Name', 'NULLABLE'),
        ('wp', 'SHORT', None, None, None, 'WP', 'NULLABLE'),
        ('wp_status', 'SHORT', None, None, None, 'WP Status', 'NULLABLE'),
        ('wp_status_text', 'TEXT', 100, None, None, 'WP Status Text', 'NULLABLE'),
        ('talar_numb', 'LONG', None, None, None, 'Talar Number', 'NULLABLE'),
        ('talar_year', 'LONG', None, None, None, 'Talar Year', 'NULLABLE'),
        ('idkun_talar_date', 'TEXT', 100, None, None, 'Idkun Talar Date', 'NULLABLE'),
        ('xoid', 'LONG', None, None, None, 'Xoid', 'NULLABLE'),
        ('gparcel', 'TEXT', 50, None, None, 'GParcel', 'NULLABLE'),
        ('globalid', 'TEXT', 38, None, None, 'Global ID', 'NULLABLE'),
        ('gdb_archive_oid', 'LONG', None, None, None, 'GDB Archive OID', 'NULLABLE'),
        ('gdb_from_date', 'DATE', None, None, None, 'GDB From Date', 'NULLABLE'),
        ('gdb_to_date', 'DATE', None, None, None, 'GDB To Date', 'NULLABLE')
    ]
    
    # Create feature class
    feature_class_path = os.path.join(gdb_path, feature_class_name)
    logger.info(f"Creating feature class: {feature_class_path}")
    
    arcpy.CreateFeatureclass_management(
        gdb_path,
        feature_class_name,
        "POLYGON",
        spatial_reference=spatial_ref,
        has_m="DISABLED",
        has_z="DISABLED"
    )
    
    # Add fields to feature class
    for field_info in fields:
        field_name, field_type, field_length, field_precision, field_scale, field_alias, field_nullable = field_info
        arcpy.AddField_management(
            feature_class_path,
            field_name,
            field_type,
            field_length=field_length,
            field_precision=field_precision,
            field_scale=field_scale,
            field_alias=field_alias,
            field_is_nullable=field_nullable
        )
    
    return feature_class_path


def upsert_blocks_and_parcels(SDE_path: Path, GDB_path: Path, field_names: list[str]):
    logger.debug("start upsert_blocks_and_parcels")
    success_count, fail_count, delete_count = 0, 0, 0
    
    try:
        # Read all destination records
        with arcpy.da.SearchCursor(str(SDE_path), field_names) as db_ops:
            blocks_and_parcels_dic_destination = {}
            for target_bp in db_ops:
                blocks_and_parcels_dic_destination[(target_bp[1], target_bp[2], target_bp[3])] = target_bp
        
        # Read all source records
        with arcpy.da.SearchCursor(str(GDB_path), field_names) as blocks_and_parcels_source:
            blocks_and_parcels_dic_source = {}
            for bp in tqdm(blocks_and_parcels_source, desc="Reading source"):
                blocks_and_parcels_dic_source[(bp[1], bp[2], bp[3])] = bp
        
        # Process updates and inserts
        logger.info(f"Processing {len(blocks_and_parcels_dic_source)} records for upsert")
        for key, bp in tqdm(blocks_and_parcels_dic_source.items(), desc="Upserting"):
            try:
                if key in blocks_and_parcels_dic_destination:
                    # Update existing record
                    where_clause = f"block_id = {bp[1]} AND parcel_id = {bp[2]} AND suffix_id = {bp[3]}"
                    with arcpy.da.UpdateCursor(str(SDE_path), field_names, where_clause) as cursor:
                        for row in cursor:
                            cursor.updateRow(bp)
                            break  # Should only be one match
                else:
                    # Insert new record
                    with arcpy.da.InsertCursor(str(SDE_path), field_names) as cursor:
                        cursor.insertRow(bp)
                success_count += 1
            except Exception:
                logger.exception(
                    f"Error in update or insert blocks_and_parcels with block_id={bp[1]}, parcel_id={bp[2]}, suffix_id={bp[3]}")
                fail_count += 1
                continue
        
        # Delete records in destination that are not in source
        to_delete_keys = set(blocks_and_parcels_dic_destination.keys()) - set(blocks_and_parcels_dic_source.keys())
        logger.info(f"Deleting {len(to_delete_keys)} records not in source")
        
        if to_delete_keys:
            with arcpy.da.UpdateCursor(str(SDE_path), field_names) as delete_cursor:
                for del_row in delete_cursor:
                    key = (del_row[1], del_row[2], del_row[3])
                    if key in to_delete_keys:
                        try:
                            delete_cursor.deleteRow()
                            delete_count += 1
                            logger.debug(f"Deleted blocks_and_parcels with block_id={del_row[1]}, parcel_id={del_row[2]}, suffix_id={del_row[3]}")
                        except Exception:
                            logger.exception(f"Error deleting blocks_and_parcels with block_id={del_row[1]}, parcel_id={del_row[2]}, suffix_id={del_row[3]}")
                            continue
        
        logger.info(
            f"Upsert complete: {success_count} successful, {fail_count} failed, {delete_count} deleted")
    except Exception as e:
        logger.exception(f"Error in upsert_blocks_and_parcels: {e}")
        raise

def main(FromCC: bool, field_names: list[str], output_dir: Path, gdb_name: str, feature_class_name: str):
    
    """Main function to process blocks_and_parcels data and create GDB."""
    
    logger.info("Starting blocks_and_parcels to GDB conversion")
    if FromCC:
        # Step 1: Connect to PostgreSQL database
        logger.info("Connecting to PostgreSQL database...")
        db_ops = DatabaseOperations(connection_type="postgres")
        
        try:
            # Step 2: Query blocks_and_parcels table where active = true
            logger.info("Querying blocks_and_parcels table...")
            
            results = db_ops.execute_query(GET_ACTIVE_FROM_CENTRAL_CATALOG)
            logger.info(f"Retrieved {len(results)} records from database")
            
            if not results:
                logger.warning("No records found with active = true")
                return
            
            # Step 3: Create GDB and feature class     
            feature_class_path = create_gdb_and_feature_class(
                output_dir, gdb_name, feature_class_name
            )
            
            # Step 4: Process data and insert into feature class
            logger.info("Processing data and inserting into feature class...")
            
            # Insert cursor
            inserted_count = 0
            error_count = 0
            
            with arcpy.da.InsertCursor(feature_class_path, field_names) as cursor:
                def generate_xoid():
                    generate_xoid.counter += 1
                    return generate_xoid.counter
                generate_xoid.counter = 0  # initialize
                for row in tqdm(results, desc="Processing records"):
                    try:
                        # Parse JSON data
                        json_data_str = row[8]  # json_data column
                        if json_data_str:
                            if isinstance(json_data_str, str):
                                json_data = json.loads(json_data_str)
                            else:
                                json_data = json_data_str
                        else:
                            json_data = {}
                        
                        # Map JSON to GDB columns
                        mapped_data = map_json_to_gdb_columns(json_data)
                        
                        # Get polygon from WKT
                        polygon_wkt = row[4]  # polygon column
                        # Use the spatial reference from the feature class
                        spatial_ref = arcpy.SpatialReference(2039)  # EPSG:2039
                        geometry = wkt_to_arcpy_geometry(polygon_wkt, spatial_ref)
                        
                        if geometry is None:
                            logger.warning(f"Skipping record {row[0]} due to invalid geometry")
                            error_count += 1
                            continue
                        
                        # Build row data for insertion
                        insert_row = [
                            geometry,  # SHAPE@
                            mapped_data.get('parcel_id'),
                            mapped_data.get('gush_num'),
                            mapped_data.get('gush_suffix'),
                            mapped_data.get('parcel'),
                            mapped_data.get('pnumtype'),
                            mapped_data.get('pnumtype_text'),
                            mapped_data.get('legal_area'),
                            mapped_data.get('status'),
                            mapped_data.get('status_text'),
                            mapped_data.get('locality_id'),
                            mapped_data.get('locality_name'),
                            mapped_data.get('reg_mun_id'),
                            mapped_data.get('reg_mun_name'),
                            mapped_data.get('county_id'),
                            mapped_data.get('county_name'),
                            mapped_data.get('region_id'),
                            mapped_data.get('region_name'),
                            mapped_data.get('wp'),
                            mapped_data.get('wp_status'),
                            mapped_data.get('wp_status_text'),
                            mapped_data.get('talar_numb'),
                            mapped_data.get('talar_year'),
                            mapped_data.get('idkun_talar_date'),
                            generate_xoid(),
                            mapped_data.get('gparcel'),
                            mapped_data.get('globalid'),
                            mapped_data.get('gdb_archive_oid'),
                            mapped_data.get('gdb_from_date'),
                            mapped_data.get('gdb_to_date')
                        ]
                        
                        cursor.insertRow(insert_row)
                        inserted_count += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing record {row[0]}: {e}")
                        error_count += 1
                        continue
            
            logger.info(f"Successfully inserted {inserted_count} records into GDB")
            if error_count > 0:
                logger.warning(f"Skipped {error_count} records due to errors")
            
            logger.info(f"GDB created successfully at: {feature_class_path}")
        finally:
            db_ops.close_connection()

    logger.info("upsert to GIS postgres starting...")

    # Step 5: Upload to GIS Postgres
    SDE_PATH = Path(postgres_SDE_path) / "GIS_PARCEL_backup"
    upsert_blocks_and_parcels(SDE_PATH, Path(output_dir, gdb_name, feature_class_name), field_names)

    logger.info("upsert to GIS postgres completed successfully.")



if __name__ == "__main__":
    output_dir = os.path.join(os.path.dirname(__file__), "outputs")
    # Get all field names for the feature class (excluding SHAPE)
    field_names = [
        'SHAPE@', 'parcel_id', 'gush_num', 'gush_suffix', 'parcel', 
        'pnumtype', 'pnumtype_text', 'legal_area', 'status', 'status_text',
        'locality_id', 'locality_name', 'reg_mun_id', 'reg_mun_name',
        'county_id', 'county_name', 'region_id', 'region_name', 'wp',
        'wp_status', 'wp_status_text', 'talar_numb', 'talar_year',
        'idkun_talar_date', 'xoid', 'gparcel', 'globalid',
        'gdb_archive_oid', 'gdb_from_date', 'gdb_to_date'
    ]
    FROM_cc = False  # Set to True to fetch from central catalog
    main(FROM_cc, field_names, output_dir, gdb_name="blocks_and_parcels", feature_class_name="blocks_and_parcels")




