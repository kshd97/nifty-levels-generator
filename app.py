import streamlit as st
import io
import os
from generate_excel import process_excel_file

# Set Page Config
st.set_page_config(page_title="Nifty Levels Generator", page_icon="📈", layout="centered")

st.title("📈 Nifty Support & Resistance Generator")
st.markdown("""
Upload your Excel file (containing daily option chain sheets like `tue`, `wed`...) 
and get the processed **Total** and **Max** levels automatically.
""")

# File Uploader
uploaded_file = st.file_uploader("Upload Excel File", type=['xlsx'])

if uploaded_file:
    # Button to process
    if st.button("Generate Levels"):
        with st.spinner("Processing..."):
            try:
                # Process the file using the refactored function
                # Read bytes from uploaded file
                input_bytes = uploaded_file.read()
                
                # Get processed output bytes
                output_bytes = process_excel_file(input_bytes)
                
                if output_bytes:
                    st.success("Processing Complete!")
                    
                    # Prepare filename
                    original_name = uploaded_file.name
                    name_root, ext = os.path.splitext(original_name)
                    output_filename = f"{name_root}_processed{ext}"
                    
                    # Download Button
                    st.download_button(
                        label="📥 Download Processed Excel",
                        data=output_bytes,
                        file_name=output_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:
                    st.error("Processing failed. Please check if the uploaded file has valid data.")
                    
            except Exception as e:
                st.error(f"An error occurred: {e}")
                
st.markdown("---")
st.markdown("### Instructions")
st.markdown("""
1.  **Upload** your daily option chain Excel file.
2.  Click **Generate Levels**.
3.  **Download** the updated file with `Total` and `Max` sheets.
""")
