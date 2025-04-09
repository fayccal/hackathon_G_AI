import xml.etree.ElementTree as ET
import os
from urllib.parse import urlparse

def get_xsd_name_from_file(xml_file_path):
    """
    Extract the XSD filename (without extension) from an XML file.
    Handles both noNamespaceSchemaLocation and schemaLocation.
    """
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        xsi_ns = "http://www.w3.org/2001/XMLSchema-instance"

        # Try noNamespaceSchemaLocation
        no_ns_location = root.attrib.get(f"{{{xsi_ns}}}noNamespaceSchemaLocation")
        if no_ns_location:
            xsd_path = no_ns_location.strip()
        else:
            # Fallback to schemaLocation (can have multiple pairs)
            schema_location = root.attrib.get(f"{{{xsi_ns}}}schemaLocation")
            if not schema_location:
                return None
            parts = schema_location.strip().split()
            if len(parts) % 2 != 0:
                print("⚠️ Unexpected schemaLocation format.")
                return None
            xsd_path = parts[-1]  # Take last as the schema file

        # Extract file name without extension
        filename = os.path.basename(urlparse(xsd_path).path)
        #return os.path.splitext(filename)[0]
        return filename

    except ET.ParseError as e:
        print(f"XML parsing error: {e}")
        return None
    except FileNotFoundError:
        print(f"File not found: {xml_file_path}")
        return None
