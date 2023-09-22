import pandas as pd 

class ACS:
    product: str = "fiveYear"
    dataURL: str = "https://allofthedata.s3.us-west-2.amazonaws.com/acs/"

    __init__(self, product:str, data_url:str ):
        if product 
            self.product = product

        if data_url:
            self.data_url = data_url

    
