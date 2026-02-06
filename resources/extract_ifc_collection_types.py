import re
import json
from pathlib import Path

ENTITY_START_RE = re.compile(r"^ENTITY (\w+)")
ENTITY_END_RE = re.compile(r"^END_ENTITY;")
TYPE_START_RE = re.compile(r"^TYPE (\w+) = SELECT")
TYPE_END_RE = re.compile(r"^END_TYPE;")
SUBTYPE_RE = re.compile(r"SUBTYPE OF \((\w+)\)")
COLL_TYPE_RE = re.compile(r"(ARRAY|LIST|SET)", re.IGNORECASE)

def parse_express_collections(exp_path):
    """Parse EXPRESS schema and extract collection types, including inherited attributes.
    
    This function performs two passes:
    1. First pass: Extract direct attributes and inheritance relationships
    2. Second pass: Resolve inherited attributes from parent classes
    
    Note: INVERSE and DERIVE attributes are excluded as they don't appear in stream2 output.
    """
    # First pass: collect direct attributes and inheritance
    direct_attrs = {}  # entity -> {attr: type}
    inheritance = {}   # entity -> parent_entity
    
    with open(exp_path, encoding="utf-8") as f:
        lines = f.readlines()
    
    current_entity = None
    inside_entity = False
    inside_inverse_or_derive = False  # Track if we're in INVERSE/DERIVE section
    
    for line in lines:
        line = line.strip()
        entity_start = ENTITY_START_RE.match(line)
        
        if entity_start:
            current_entity = entity_start.group(1)
            direct_attrs[current_entity] = {}
            inside_entity = True
            inside_inverse_or_derive = False
            continue
            
        if inside_entity:
            if ENTITY_END_RE.match(line):
                inside_entity = False
                inside_inverse_or_derive = False
                current_entity = None
                continue
            
            # Check if we're entering INVERSE or DERIVE section
            if line.startswith(('INVERSE', 'DERIVE')):
                inside_inverse_or_derive = True
                continue
            
            # Check if we're exiting INVERSE/DERIVE section (WHERE or other keywords)
            if line.startswith(('WHERE', 'UNIQUE')):
                inside_inverse_or_derive = False
                continue
            
            # Skip attributes in INVERSE or DERIVE sections
            if inside_inverse_or_derive:
                continue
            
            # Check for SUBTYPE OF declaration
            subtype_match = SUBTYPE_RE.search(line)
            if subtype_match and current_entity:
                parent = subtype_match.group(1)
                inheritance[current_entity] = parent
                continue
            
            # Only match attribute lines (not SUPERTYPE, SUBTYPE, END_ENTITY, etc.)
            if ':' in line and not line.startswith(('SUPERTYPE', 'SUBTYPE', 'END_ENTITY')):
                attr_name = line.split(':', 1)[0].strip()
                # Find collection type
                coll_match = COLL_TYPE_RE.search(line)
                if coll_match and current_entity:
                    coll_type = coll_match.group(1).upper()
                    direct_attrs[current_entity][attr_name] = coll_type
    
    # Second pass: resolve inheritance chain for each entity
    def get_all_attrs(entity_name, visited=None):
        """Recursively collect all attributes including inherited ones."""
        if visited is None:
            visited = set()
        
        if entity_name in visited:
            return {}  # Circular reference protection
        visited.add(entity_name)
        
        # Start with direct attributes
        all_attrs = dict(direct_attrs.get(entity_name, {}))
        
        # Add parent attributes (parents take precedence if there's overlap)
        if entity_name in inheritance:
            parent_name = inheritance[entity_name]
            parent_attrs = get_all_attrs(parent_name, visited)
            # Parent attributes don't override child attributes
            for attr, coll_type in parent_attrs.items():
                if attr not in all_attrs:
                    all_attrs[attr] = coll_type
        
        return all_attrs
    
    # Build final mapping with inherited attributes
    mapping = {}
    for entity in direct_attrs.keys():
        mapping[entity] = get_all_attrs(entity)
    
    return mapping


def parse_express_select_types(exp_path):
    """Parse EXPRESS schema and extract SELECT type definitions.
    
    This identifies which attributes use SELECT types (like IfcValue, IfcUnit, etc.)
    so we can properly handle typed values in stream2 output.
    
    Returns:
        dict: Maps SELECT type names to their constituent types
              e.g., {'IfcValue': ['IfcDerivedMeasureValue', 'IfcMeasureValue', ...]}
    """
    select_types = {}
    
    with open(exp_path, encoding="utf-8") as f:
        lines = f.readlines()
    
    current_type = None
    inside_select = False
    
    for line in lines:
        line = line.strip()
        
        # Check for TYPE ... = SELECT
        type_start = TYPE_START_RE.match(line)
        if type_start:
            current_type = type_start.group(1)
            select_types[current_type] = []
            inside_select = True
            continue
        
        if inside_select:
            if TYPE_END_RE.match(line):
                inside_select = False
                current_type = None
                continue
            
            # Extract type names from SELECT list
            # Format: (IfcType1, IfcType2, ...)
            if '(' in line or ',' in line:
                # Remove parentheses and split by comma
                types_str = line.replace('(', '').replace(')', '').replace(';', '')
                for type_name in types_str.split(','):
                    type_name = type_name.strip()
                    if type_name and type_name.startswith('Ifc'):
                        select_types[current_type].append(type_name)
    
    return select_types


def find_select_attributes(exp_path, select_types):
    """Find which entity attributes use SELECT types.
    
    Args:
        exp_path: Path to EXPRESS schema file
        select_types: Dict of SELECT type definitions from parse_express_select_types
        
    Returns:
        dict: Maps entity names to their SELECT attributes
              e.g., {'IfcPropertySingleValue': {'NominalValue': 'IfcValue', 'Unit': 'IfcUnit'}}
    """
    entity_select_attrs = {}
    inheritance = {}
    
    with open(exp_path, encoding="utf-8") as f:
        lines = f.readlines()
    
    current_entity = None
    inside_entity = False
    inside_inverse_or_derive = False
    
    for line in lines:
        line = line.strip()
        entity_start = ENTITY_START_RE.match(line)
        
        if entity_start:
            current_entity = entity_start.group(1)
            entity_select_attrs[current_entity] = {}
            inside_entity = True
            inside_inverse_or_derive = False
            continue
        
        if inside_entity:
            if ENTITY_END_RE.match(line):
                inside_entity = False
                inside_inverse_or_derive = False
                current_entity = None
                continue
            
            # Check if we're entering INVERSE or DERIVE section
            if line.startswith(('INVERSE', 'DERIVE')):
                inside_inverse_or_derive = True
                continue
            
            # Check if we're exiting INVERSE/DERIVE section
            if line.startswith(('WHERE', 'UNIQUE')):
                inside_inverse_or_derive = False
                continue
            
            # Skip attributes in INVERSE or DERIVE sections
            if inside_inverse_or_derive:
                continue
            
            # Check for SUBTYPE OF declaration
            subtype_match = SUBTYPE_RE.search(line)
            if subtype_match and current_entity:
                parent = subtype_match.group(1)
                inheritance[current_entity] = parent
                continue
            
            # Match attribute lines: AttributeName : OPTIONAL TypeName
            if ':' in line and not line.startswith(('SUPERTYPE', 'SUBTYPE', 'END_ENTITY')):
                parts = line.split(':', 1)
                if len(parts) == 2:
                    attr_name = parts[0].strip()
                    type_part = parts[1].strip()
                    
                    # Check if type is a SELECT type (or contains one)
                    for select_name in select_types.keys():
                        if select_name in type_part:
                            entity_select_attrs[current_entity][attr_name] = select_name
                            break
    
    # Resolve inheritance for SELECT attributes
    def get_all_select_attrs(entity_name, visited=None):
        if visited is None:
            visited = set()
        if entity_name in visited:
            return {}
        visited.add(entity_name)
        
        all_attrs = dict(entity_select_attrs.get(entity_name, {}))
        
        if entity_name in inheritance:
            parent_name = inheritance[entity_name]
            parent_attrs = get_all_select_attrs(parent_name, visited)
            for attr, sel_type in parent_attrs.items():
                if attr not in all_attrs:
                    all_attrs[attr] = sel_type
        
        return all_attrs
    
    # Build final mapping with inherited attributes
    final_mapping = {}
    for entity in entity_select_attrs.keys():
        attrs = get_all_select_attrs(entity)
        if attrs:  # Only include entities that have SELECT attributes
            final_mapping[entity] = attrs
    
    return final_mapping


def main():
    """Generate collection and SELECT type maps for IFC schemas.
    
    Extracts:
    1. Collection types (LIST, SET, ARRAY) from entity attributes
    2. SELECT type definitions and which attributes use them
    
    Generates Python and JSON files for runtime use.
    """
    schemas = [
        ("ifc2x3", "resources/ifc_schemas/IFC2X3.exp"),
        ("ifc4", "resources/ifc_schemas/IFC4.exp"),
        ("ifc4x3_add2", "resources/ifc_schemas/IFC4X3_ADD2.exp"),
    ]
    
    from datetime import datetime
    timestamp = datetime.now().isoformat()
    
    for schema_name, exp_path in schemas:
        # Extract collection types
        collection_mapping = parse_express_collections(exp_path)
        
        # Extract SELECT types
        select_types = parse_express_select_types(exp_path)
        select_attr_mapping = find_select_attributes(exp_path, select_types)
        
        # Write collection types (existing functionality)
        py_path = f"resources/ifc_schemas/{schema_name}_collection_types.py"
        json_path = f"resources/ifc_schemas/{schema_name}_collection_types.json"
        
        with open(py_path, "w", encoding="utf-8") as f:
            f.write(f'"""Collection type mapping for {schema_name.upper()}.\n\n')
            f.write(f'Auto-generated from {exp_path}\n')
            f.write(f'Generated: {timestamp}\n\n')
            f.write('Maps entity types to their collection attributes (LIST, SET, ARRAY).\n')
            f.write('Includes inherited attributes from parent classes.\n')
            f.write('Excludes INVERSE and DERIVE attributes (not present in stream2 output).\n')
            f.write('"""\n\n')
            f.write("COLLECTION_TYPE_MAP = ")
            f.write(repr(collection_mapping))
        
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(collection_mapping, f, indent=2)
        
        # Write SELECT types (new functionality)
        select_py_path = f"resources/ifc_schemas/{schema_name}_select_types.py"
        select_json_path = f"resources/ifc_schemas/{schema_name}_select_types.json"
        
        with open(select_py_path, "w", encoding="utf-8") as f:
            f.write(f'"""SELECT type mapping for {schema_name.upper()}.\n\n')
            f.write(f'Auto-generated from {exp_path}\n')
            f.write(f'Generated: {timestamp}\n\n')
            f.write('Maps entity types to attributes that use SELECT types.\n')
            f.write('Includes inherited attributes from parent classes.\n')
            f.write('SELECT types create typed entities in TTL output (e.g., inst:ref_123_t1).\n')
            f.write('"""\n\n')
            f.write("# SELECT type definitions (what types are in each SELECT)\n")
            f.write("SELECT_TYPE_DEFINITIONS = ")
            f.write(repr(select_types))
            f.write("\n\n")
            f.write("# Entity attributes that use SELECT types\n")
            f.write("SELECT_ATTRIBUTE_MAP = ")
            f.write(repr(select_attr_mapping))
        
        with open(select_json_path, "w", encoding="utf-8") as f:
            json.dump({
                "select_type_definitions": select_types,
                "select_attribute_map": select_attr_mapping
            }, f, indent=2)
        
        print(f"✓ Collections: {py_path}")
        print(f"✓ SELECT types: {select_py_path}")
        print()

if __name__ == "__main__":
    main()
