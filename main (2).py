# main.py
from fastapi import FastAPI, Query, HTTPException
from typing import Optional, List
from utils.search import search_data, search_datav2

app = FastAPI()

# @app.get("/search")
# async def search(input_string: Optional[str] = Query(None, description="External IDs separated by comma, space, or semicolon")):
#     """
#     Search endpoint that accepts invoice numbers as a query parameter with separators (comma, space, semicolon)
#     """
#     try:
#         if not input_string:
#             raise HTTPException(status_code=400, detail="Input string cannot be empty")
        
#         # Define separators and clean input
#         separators = [',', ';', ' ']  # Define separators
#         input_cleaned = input_string
#         for sep in separators:
#             input_cleaned = input_cleaned.replace(sep, ',')  # Replace all separators with a single comma
        
#         # Split the cleaned string into a list
#         invoice_numbers = [num.strip() for num in input_cleaned.split(',') if num.strip()]
        
#         # Perform the search
#         result = search_data(list_external_id = invoice_numbers)

#         return {
#             "status": "success",
#             "external_ids": invoice_numbers,
#             "results": result  # Your search results will go here
#         }
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Error processing input: {str(e)}")
        
@app.get("/search")    
async def search(
    input_string: Optional[str] = Query(None, description="External IDs separated by comma, space, or semicolon"),
    input_invoice: Optional[str] = Query(None, description="Invoice Numbers separated by comma, space, or semicolon")
):
    """
    Search endpoint that accepts external IDs and invoice numbers as query parameters with separators (comma, space, semicolon)
    """
    if not input_string and not input_invoice:
        raise HTTPException(status_code=400, detail="Both input_string and input_invoice cannot be empty")

    # Function to split and clean input strings
    def split_and_clean(input_data: Optional[str]) -> List[str]:
        if not input_data:
            return []
        separators = [',', ';', ' ']
        for sep in separators:
            input_data = input_data.replace(sep, ',')
        return [item.strip() for item in input_data.split(',') if item.strip()]

    # Process both inputs
    external_ids = split_and_clean(input_string)
    invoice_numbers = split_and_clean(input_invoice)

    # Call the search function
    result = search_datav2(list_invoice_number=invoice_numbers, list_external_id=external_ids)
        
    
    return {
        "status": "success",
        "external_ids": invoice_numbers,
        "results": result  # Your search results will go here
    }

    
    try:
        if not input_string:
            raise HTTPException(status_code=400, detail="Input string cannot be empty")
        
        # Define separators and clean input
        separators = [',', ';', ' ']  # Define separators
        input_cleaned = input_string
        for sep in separators:
            input_cleaned = input_cleaned.replace(sep, ',')  # Replace all separators with a single comma
        
        # Split the cleaned string into a list
        invoice_numbers = [num.strip() for num in input_cleaned.split(',') if num.strip()]
        
        # Perform the search
        result = search_datav2(list_external_id = invoice_numbers)

        return {
            "status": "success",
            "external_ids": invoice_numbers,
            "results": result  # Your search results will go here
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing input: {str(e)}")
