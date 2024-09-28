import requests
import os
import time
from datetime import datetime, timedelta


base_url_1 = 'https://www.nseindia.com/api/reports?archives=[{\"name\":\"CM-UDiFF Common Bhavcopy Final (zip)\",\"type\":\"daily-reports\",\"category\":\"capital-market\",\"section\":\"equities\"}]&date='

base_url_2 = '&type=equities&mode=single'

start_date = datetime(2001, 1, 1)
end_date = datetime(2024, 8, 30)

current_date = start_date



#url = "https://www.nseindia.com/api/reports?archives=[{\"name\":\"CM-UDiFF Common Bhavcopy Final (zip)\",\"type\":\"daily-reports\",\"category\":\"capital-market\",\"section\":\"equities\"}]&date=13-Sep-2024&type=equities&mode=single"
    

def download_bhavcopy(url, folder_path, date):
    
    # Headers and cookies from the raw HTTP request
    headers = {
        "Host": "www.nseindia.com",
        "Cookie": "_ga=GA1.1.241952520.1725424244; nsit=_ouBKzuj1LbK5UMnHf8wg-Md; AKA_A2=A; _abck=1C6009C2F2EB8135236ADD9C55FE65A3~0~YAAQh+scuLZHZteRAQAAuh1w+gx8m2PZqBMiROD3OFPrcgBtZJTUqgtrU2cVBJfh3laxHO7wZwu9ZGz+LOyK4sm9V2SoTojauHyq4KJc5bM6bSCja0Lc7nYGvimtMfW1fMBM4/BxwfCP3pjxn+StV3aXZQEmA3nN02G2E9cd/MdtssbgfMdk177M/vEMGYdl/JWaCFLyv1Tl2WdtxOThoGPmQe5rAxpuemMoRU+ygxd0UWjR0S839mmWH4Bf/LEpAGivS1GlTpfeEzh7No42AU+P9sYKOho1OsMTrWbCS0xsLrJkVzEVp1yrQwyySr6Hph31cXsyWIR227ykwYp38SU/KbeK1yUUVfYwQoBAfEqKoicdTYyJ37lLGoBa7Dql/nXBRl2AiT0dUHe8/biTl3MYooQ0P7Hrsi+EDjZCdNN2QOk68YbVd6Yf78K8DMMQHpkd4x8HMVMhyPw=~-1~-1~-1; defaultLang=en; ak_bmsc=C9E4F66DD7646B1EBD8BB775534B4988~000000000000000000000000000000~YAAQh+scuPFHZteRAQAAoilw+hllVVL+QXfP1au+PdhDeb5WMi1dzIqdMSeR7V/z6GxVA1t4O3fu0MO7ROoH/1UAo7UGfYE2hSvPOaaUgG3ViqvqtwJZrAmj5yLvOD8QaGCCYVMS21B58Bnh6em1tGS5WGUuHRvb2L+JaOJgWnhcEXYJRoV/JnRO+ObeM+n5I5NE23RMM7RxZRY+ITHVsppw9bmk2ILgqqXdXLB6Uz6msYZTMgvxwcxqnIVE5MegAOd3Y1ogcNsO1EmRP+HiCe4mxnrQ3hjirXLpvpOwvnU1mSAGEtfa5rmX2tq0qVoFSHwCcWYM2f/MJIZayOWMq4jxNc6JST196rDRF49JERZjpSwEAR2ascrOmdi4lmr04Nk20vF0JCPRKQK/DkU1Ae/ikgfdfUbBey0J6iXBg7oI5jUgJO2U8rR4HOKAUJoHCb1oN3aroXXUWsBemHFcT0w=; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTcyNjQ4MzU0MCwiZXhwIjoxNzI2NDkwNzQwfQ.VKgDYApDey8vKZ2E9ocXWdNm7bdryfyY3DxBNtXGR0Y; bm_sz=4C14B2173B7A1B08CB049B8AAF977F7A~YAAQh+scuPpHZteRAQAAQCxw+hn7OeWkoICoNUDB/FfX0+E8EsoPprCLIOp2VXcN2dgIKe6Q0IfbQ154k+U0CNvRRz4OZTW2B57voQK9IpkbX34QkQa0LxT9R4b3tiJuGgZQhx1mvLXUhVbn2lDb9TXeb2zfqSlmVgcTLpGZHe7tXL64695J0PVBgOfyvH+2H40LPYNLstKd30CREFdQc0Vq6W9fzyNqI45V7Ao9i8D3OD8IarivSQxET0Mc6GE/V0KLjTnUoHdMPGL6IofOX3SWD4fZiCebx1oivOvbNCvO9Q1/pxkqOZE2g0RTekcY6U6iagToe6YeTj9hfIgI9VK20ogoHbk63ocNGEJ4OkUnSNBmby3GvktcVVMEYh+lnwxZufnJJOIMwKYsF76zdPGijty9~4277825~3552313; _ga_87M7PJ3R97=GS1.1.1726483538.4.1.1726483539.59.0.0; bm_sv=C7EA425007B81371D9A8B8F292A63626~YAAQh+scuEhIZteRAQAAaDhw+hmv+h6pekkFuz6+8+0y4BTpO/wYdmNq3M0FtvWV1b/o/AJchnoNzmyIqW+kNb3XzuK6BAgbzzFhaqasgTTfFI5moqghBgXSwNGk6wWjDfuR0CMXwHzv8h/+UJ157aCBwAMlmYqaduIsMhIrhoYtTzKByh3yN3AbxJ46EDoUlvE7SzKsl6LOYrSGAmE5eNCyfWHs3bOCEn7MZXdpDEyUmE08vLbLOKAHC7iyhDtO4WY=~1; RT=\"z=1&dm=nseindia.com&si=c91a099d-314a-4ea2-a7eb-c5f9ac6b9e59&ss=m14vpotm&sl=1&se=8c&tt=137&bcn=%2F%2F684d0d48.akstat.io%2F&ld=d8d&nu=kpaxjfo&cl=m8z\"",
        "Sec-Ch-Ua": '"Chromium";v="127", "Not)A;Brand";v="99"',
        "Accept": "*/*",
        "X-Requested-With": "XMLHttpRequest",
        "Accept-Language": "en-GB",
        "Sec-Ch-Ua-Mobile": "?0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.6533.100 Safari/537.36",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https://www.nseindia.com/all-reports",
        "Accept-Encoding": "gzip, deflate, br",
        "Priority": "u=1, i"
    }
    
    cookies = {
        "_ga": "GA1.1.241952520.1725424244",
        "nsit": "_ouBKzuj1LbK5UMnHf8wg-Md",
        "AKA_A2": "A",
        "_abck": "1C6009C2F2EB8135236ADD9C55FE65A3~0~YAAQh+scuLZHZteRAQAAuh1w+gx8m2PZqBMiROD3OFPrcgBtZJTUqgtrU2cVBJfh3laxHO7wZwu9ZGz+LOyK4sm9V2SoTojauHyq4KJc5bM6bSCja0Lc7nYGvimtMfW1fMBM4/BxwfCP3pjxn+StV3aXZQEmA3nN02G2E9cd/MdtssbgfMdk177M/vEMGYdl/JWaCFLyv1Tl2WdtxOThoGPmQe5rAxpuemMoRU+ygxd0UWjR0S839mmWH4Bf/LEpAGivS1GlTpfeEzh7No42AU+P9sYKOho1OsMTrWbCS0xsLrJkVzEVp1yrQwyySr6Hph31cXsyWIR227ykwYp38SU/KbeK1yUUVfYwQoBAfEqKoicdTYyJ37lLGoBa7Dql/nXBRl2AiT0dUHe8/biTl3MYooQ0P7Hrsi+EDjZCdNN2QOk68YbVd6Yf78K8DMMQHpkd4x8HMVMhyPw=~-1~-1~-1",
        "defaultLang": "en",
        "ak_bmsc": "C9E4F66DD7646B1EBD8BB775534B4988~000000000000000000000000000000~YAAQh+scuPFHZteRAQAAoilw+hllVVL+QXfP1au+PdhDeb5WMi1dzIqdMSeR7V/z6GxVA1t4O3fu0MO7ROoH/1UAo7UGfYE2hSvPOaaUgG3ViqvqtwJZrAmj5yLvOD8QaGCCYVMS21B58Bnh6em1tGS5WGUuHRvb2L+JaOJgWnhcEXYJRoV/JnRO+ObeM+n5I5NE23RMM7RxZRY+ITHVsppw9bmk2ILgqqXdXLB6Uz6msYZTMgvxwcxqnIVE5MegAOd3Y1ogcNsO1EmRP+HiCe4mxnrQ3hjirXLpvpOwvnU1mSAGEtfa5rmX2tq0qVoFSHwCcWYM2f/MJIZayOWMq4jxNc6JST196rDRF49JERZjpSwEAR2ascrOmdi4lmr04Nk20vF0JCPRKQK/DkU1Ae/ikgfdfUbBey0J6iXBg7oI5jUgJO2U8rR4HOKAUJoHCb1oN3aroXXUWsBemHFcT0w=",
        "nseappid": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTcyNjQ4MzU0MCwiZXhwIjoxNzI2NDkwNzQwfQ.VKgDYApDey8vKZ2E9ocXWdNm7bdryfyY3DxBNtXGR0Y",
        "bm_sz": "4C14B2173B7A1B08CB049B8AAF977F7A~YAAQh+scuPpHZteRAQAAQCxw+hn7OeWkoICoNUDB/FfX0+E8EsoPprCLIOp2VXcN2dgIKe6Q0IfbQ154k+U0CNvRRz4OZTW2B57voQK9IpkbX34QkQa0LxT9R4b3tiJuGgZQhx1mvLXUhVbn2lDb9TXeb2zfqSlmVgcTLpGZHe7tXL64695J0PVBgOfyvH+2H40LPYNLstKd30CREFdQc0Vq6W9fzyNqI45V7Ao9i8D3OD8IarivSQxET0Mc6GE/V0KLjTnUoHdMPGL6IofOX3SWD4fZiCebx1oivOvbNCvO9Q1/pxkqOZE2g0RTekcY6U6iagToe6YeTj9hfIgI9VK20ogoHbk63ocNGEJ4OkUnSNBmby3GvktcVVMEYh+lnwxZufnJJOIMwKYsF76zdPGijty9~4277825~3552313",
        "_ga_87M7PJ3R97": "GS1.1.1726483538.4.1.1726483539.59.0.0",
        "bm_sv": "C7EA425007B81371D9A8B8F292A63626~YAAQh+scuEhIZteRAQAAaDhw+hmv+h6pekkFuz6+8+0y4BTpO/wYdmNq3M0FtvWV1b/o/AJchnoNzmyIqW+kNb3XzuK6BAgbzzFhaqasgTTfFI5moqghBgXSwNGk6wWjDfuR0CMXwHzv8h/+UJ157aCBwAMlmYqaduIsMhIrhoYtTzKByh3yN3AbxJ46EDoUlvE7SzKsl6LOYrSGAmE5eNCyfWHs3bOCEn7MZXdpDEyUmE08vLbLOKAHC7iyhDtO4WY=~1",
    }
    
    # Make the request
    response = requests.get(url, headers=headers, cookies=cookies)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Save the file
        file_path = os.path.join(folder_path, date+'.zip')
        
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print("File downloaded successfully!")
    else:
        print(f"Failed to download file. Status code: {response.status_code}, Response: {response.text}")

folder_path = os.getcwd() + '\\Bhavcopy'

# Call the function
#download_bhavcopy(url, folder_path)


while current_date <= end_date:
    
    if current_date.weekday() < 5:
        date_str = current_date.strftime("%d-%b-%Y")
        
        url = base_url_1+date_str+base_url_2
        
        print(url)
        #try:
        #    download_bhavcopy(url, folder_path, date_str)
        #    time.sleep(5)
        #except Exception as e:
        #    print(e)
    current_date += timedelta(days=1)