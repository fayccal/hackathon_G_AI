import langgraph
import langgraph.graph
from lxml import etree
import xmlschema
from typing import TypedDict, Optional, Any
from mod import get_xsd_name_from_file
import streamlit as st

class WorkflowState(TypedDict):
    xml_path: str
    xsd_path: str
    xml_tree: Optional[Any]
    modified_xml: Optional[Any]
    is_valid: Optional[bool]
    error: Optional[str]
    
def is_it_valid(xml_path):
    xml_path = "data/" + xml_path
    tree = etree.parse(xml_path)
    xsd = get_xsd_name_from_file(xml_path)
    xsd_path = "xml_schema_flat/" + xsd 

    schema = xmlschema.XMLSchema(xsd_path)
    is_valid = schema.is_valid(tree)
    return is_valid



st.markdown(
    "<h1 style='text-align: center;'>XML S1000D Validation </h1>",
    unsafe_allow_html=True)

with st.container():
    with st.expander("Enter the name of the XML File in the data folder", expanded=True):
        customer_request = st.text_area('XML',"""""", height=70)

with st.container():
    _,mid_col,_ = st.columns([.4,.3,.3])
    with mid_col:
        generate_template = st.button('Check Validation ⚡',type="primary")

with st.container():
    if generate_template:
        if len(customer_request.strip()) != 0:
            state_valid = is_it_valid(customer_request)
            if state_valid == True:
                state_valid = "Valide"
            else:
                state_valid = "Invalide"
            st.write("Le fichier XML est {} par le XSD du standard S1000D.".format(state_valid))