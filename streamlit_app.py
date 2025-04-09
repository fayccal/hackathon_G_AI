
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import complete
import json
import streamlit as st
import ast
import base64
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
import os

def get_contenu_from_table(xsd_name:str):
    # retourn le contenu d'un xsd
    df = session.table("XSD_FILES_2")
    
    filtered_df = df.filter(df["nom_xsd"] == xsd_name).select("contenu")
    
    # Collect result and extract value
    rows = filtered_df.collect()
    
    if rows:
        contenu_value = rows[0]["CONTENU"]  # Case-sensitive key
        return contenu_value
        #print("Contenu for file_xsd = 'proced':", contenu_value)
    else:
        return ""


def extract_imported_xsds(xsd_string):
    #extract les imports xsd
    try:
        root = ET.fromstring(xsd_string)
        imports = []

        # Parcours des éléments <xs:import> pour extraire les schémas importés
        for elem in root.iter():
            if elem.tag.endswith('import'):
                schema_location = elem.attrib.get('schemaLocation')
                if schema_location:
                    # Extraction du nom du fichier sans l'extension .xsd
                    filename = os.path.basename(schema_location.strip())
                    name_without_extension = os.path.splitext(filename)[0]
                    imports.append(name_without_extension)

        return imports
    except ET.ParseError as e:
        print(f"Erreur lors de l'analyse du XSD : {e}")
        return []
        

def extract_xsd_name(xml_string):
    # extract le xsd du xml
    try:
        root = ET.fromstring(xml_string)
        xsi_ns = "http://www.w3.org/2001/XMLSchema-instance"

        # Get noNamespaceSchemaLocation
        no_ns_location = root.attrib.get(f"{{{xsi_ns}}}noNamespaceSchemaLocation")
        schema_url = None

        if no_ns_location:
            schema_url = no_ns_location.strip()
        else:
            # Fallback to schemaLocation if needed
            schema_location = root.attrib.get(f"{{{xsi_ns}}}schemaLocation")
            if schema_location:
                parts = schema_location.strip().split()
                if parts:
                    schema_url = parts[-1]  # Assume last is the schema URL

        if schema_url:
            filename = os.path.basename(urlparse(schema_url).path)
            return os.path.splitext(filename)[0]  # Return filename without .xsd

        return None

    except ET.ParseError as e:
        print(f"XML parsing error: {e}")
        return None


def get_all_xsd_content(xml):

    xsd_prin_name = extract_xsd_name(xml)
    xsd_princ_content = get_contenu_from_table(xsd_prin_name)
    import_names = extract_imported_xsds(xsd_princ_content)
    importes_content = []
    for i in import_names:
        importes_content.append(get_contenu_from_table(i))
        
    full_xsd = "This is the main xsd:\n"
    full_xsd = full_xsd + xsd_princ_content + "\n\n"
    
    for i in importes_content:
        full_xsd = full_xsd + "One of the imports of the xsd : \n" + i + "\n"
    return full_xsd

##############################################################################
##############################################################################
##############################################################################


session = get_active_session()


def clean_xml_string(s):
    # Remove starting ```xml if present
    if s.startswith("```xml"):
        s = s[len("```xml"):].lstrip()

    # Remove ending ``` if present
    if s.endswith("```"):
        s = s[:-3].rstrip()

    return s

def clean_xml_string_simple(s):
    return s.replace("```xml", "").replace("```", "").strip()

from snowflake.snowpark.functions import col

# 3. Sort the table to get the "last" row — you need to define what "last" means (usually based on a timestamp or ID)
# Replace 'your_column' with the appropriate column to sort by
rows = session.table("TEST_CASE2").collect()

# Get the last row
last_row = rows[-1]
print(last_row)
# Access the first element of the last row
original_example = last_row[0]
modif_example = last_row[1]
instruction_example = last_row[2]


prompt_template = """You are an expert in technical documentation and XML structuring, specializing in the S1000D standard.
Your task is to modify the provided XML document by applying the requested instructions, which may include deletion and insertion
while strictly adhering to the provided XSD schema.


The provided XML document is as follows: <XMLDOC>{the_xml}</XMLDOC>

The modifications to be applied to the XML document are the following instructions: <modification>{modification}</modification>.

The XSD document to respect and it's imports are the following: <XSD>{full_xsd}</XSD>

<output>
You must return **only the complete and well-formed XML document** after all modifications have been applied.

- The output must include the **entire XML document** from the first line (e.g., `<?xml...?>`) to the last closing tag with the entire content.
- **All unchanged parts of the document must be preserved and present.**
- All specified modifications must be included and applied in the appropriate locations.
- The XML must be syntactically valid, with all tags properly opened and closed.
- Do not add comments in the xml.
- Do not return any explanations, notes, or extra text — only the final modified XML document.
</output>

"""





continue_prompt = """
You are an expert in technical documentation and XML structuring, specializing in the S1000D standard.

You previously started modifying the provided XML document according to specific instructions and in compliance with the given XSD schema.

Here is the partial output generated so far:
<partial_output>
{previous_output}
</partial_output>

Continue generating the modified XML from where you left off, preserving all previous content and continuing seamlessly.

Do not return any explanation or additional information — only the next part of the modified XML document.
Stop exactly when you reach the end of the complete modified XML document, or when the output limit is reached.

Context:
Original XML document:
<XMLDOC>
{the_xml}
</XMLDOC>

Modifications to be applied:
<modification>
{modification}
</modification>

The XSD schema and its imports to comply with:
<XSD>
{full_xsd}
</XSD>
"""

st.markdown(
    "<h1 style='text-align: center;'>XML editing by generative AI</h1>",
    unsafe_allow_html=True
)


with st.container():
    with st.expander("Enter xml, instructions and select LLM", expanded=True):
        xml_given = st.text_area('XML',""" """, height=300)

        with st.container():
            edit_request = st.text_area("Edit instructions", """ """, height=130)
        with st.container():
            #left_col, right_col = st.columns(2)
            #with left_col:
            #    selected_preference = st.selectbox('Select contact preference', ('Text message', 'Email'))
            #with right_col:
            selected_llm = st.selectbox('Select LLM',('claude-3-5-sonnet'))

with st.container():
    _,mid_col,_ = st.columns([.4,.3,.3])
    with mid_col:
        generate_template = st.button('Generate new xml ⚡',type="primary")

query = """
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    TO_VARIANT(ARRAY_CONSTRUCT(
        OBJECT_CONSTRUCT(
            'role', 'user',
            'content', ?
        )
    )),
    OBJECT_CONSTRUCT('max_tokens', 8192)
) AS result;
"""

# Avec Snowpark session

with st.container():
    if generate_template:
        status = st.empty()
        status.write("Loading")
        if len(xml_given) < 10 or len(edit_request) < 10:
            st.write("You have to enter both the xml and the edit request.")
        else:
            full_xsd = get_all_xsd_content(xml_given)
            prompt_xml = prompt_template.format(the_xml=xml_given, modification=edit_request, full_xsd=full_xsd)
            #prompt_xml = prompt_template.format(original_example=original_example,instruction_example=instruction_example,modif_example=modif_example,
            #                                    the_xml=xml_given, modification=edit_request, full_xsd=full_xsd)

            #edited_xml = complete(selected_llm, prompt_xml)
            df = session.sql(query, params=[prompt_xml])
            result = df.collect()[0]['RESULT']
            response = json.loads(result)  # convertir en dictionnaire Python
            edited_xml = response['choices'][0]['messages']
            nb_token = response['usage']["completion_tokens"]

            continue_xml = None
            #continue part
            if nb_token > 4080:
                edited_xml = edited_xml.rsplit('\n', 1)[0]
                next_prompt = continue_prompt.format(previous_output=edited_xml, the_xml=xml_given, modification=edit_request, full_xsd=full_xsd)
                df = session.sql(query, params=[next_prompt])
                result = df.collect()[0]['RESULT']
                response = json.loads(result)  # convertir en dictionnaire Python
                continue_xml = response['choices'][0]['messages']
                continue_xml = clean_xml_string_simple(continue_xml)
                nb_token = response['usage']["completion_tokens"]
            
            
            st.subheader("Edited XML:")
            #st.text_area('XML edited', edited_xml, height=300)
            #st.code(edited_xml)
            if continue_xml != None:
                st.code(edited_xml + "\n" + continue_xml)
            else:
                st.code(edited_xml)

            status.write('Done')
            
            #st.write(edited_xml)


