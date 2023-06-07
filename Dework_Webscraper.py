from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as E
import requests
from webdriver_manager.chrome import ChromeDriverManager
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import StaleElementReferenceException
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from selenium.webdriver.chrome.options import Options





## Initialize Selenium Browser
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')  # Last I checked this was necessary.
driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
driver.maximize_window()



## Get to main dework site 

url = 'https://app.dework.xyz/'
driver.get(url)
ant_table_wait = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'ant-tabs-content-holder')))
ant_table = driver.find_element(By.CLASS_NAME, 'ant-tabs-content-holder')

## Find the total number of pages containing DAO's 
max_pages = int(driver.find_element(By.XPATH, '/html/body/div/section/main/section/main/div[3]/div[2]/div/div[1]/div[2]/div/div/div/ul/li[8]').text)
top_dao_pages = []
for i in range(1, max_pages):
    top_dao_pages.append('https://app.dework.xyz/?page=' +str(i))



# # Get URL for each DAO
urls = []
for page in top_dao_pages:
    driver.get(page)
    wait = WebDriverWait(driver, 10)
    ant_table_wait = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'ant-table-container')))
    ant_table = driver.find_element(By.CLASS_NAME, 'ant-table-container')
    
    
    rows = driver.find_elements(By.CSS_SELECTOR, ".ant-table-cell > a")
    for row in rows:
        urls.append(row.get_attribute('href'))
        time.sleep(5)











## Scrape dao, task_title, status, priority, assignee, bounty, task description, activity and subtasks from each row. For non-mandatory fields, exception for 'na' if null value



def extract_data():
    global driver
    
    page_source = driver.page_source
    dao_url = driver.current_url
    
    
    d = dao_url.split('/')
    dao_name= d[3]
    
    soup = BeautifulSoup(page_source, 'html.parser')
    main_div = soup.find("div", {"class": "ant-modal-body"})
   
    
    try:
        view_more = driver.find_element('xpath', '//*[@id="Task Form (update)"]/section/div/div/div[3]/div/button')
        view_more.click()
        
    except:
        pass
    
    

    ## Find title
    textarea = main_div.find('textarea', {'id': 'Task Form (update)_name'})
    task_title = textarea.text.strip()
    
    ## Find status
    status_element = main_div.find('div', {'class': 'ant-col ant-form-item-label'},string=lambda text: text.strip() == 'Status').parent
    status_div = status_element.find('div', {'class': 'ant-select-selector'})
    status = status_div.find('span', {'class': 'ant-select-selection-item'}).text
    
    ## Find Priority
    priority_element = main_div.find('div', {'class': 'ant-col ant-form-item-label'},string=lambda text: text.strip() == 'Priority').parent
    priority_div = priority_element.find('div', {'class': 'ant-select-selector'})
    priority = priority_div.find('span', {'class': 'ant-select-selection-item'}).text


 ## Find Assignee
    try:
        assignee_element = main_div.find('div', {'class': 'ant-col ant-form-item-label'},string=lambda text: text.strip() == 'Assignee').parent
        assignee_div = assignee_element.find('div', {'class': 'ant-select-selector'})

        assignee = assignee_div.find('span', class_='ant-typography').text

    except:
        assignee = 'n/a'

    
    ## Find Bounty
    try:

        bounty_element = driver.find_element('xpath', '//*[@id="Task Form (update)"]/section/div/div/div[2]/div[1]/span')
        bounty = bounty_element.text
                            
        
    except:
        
        bounty = 'n/a'
        

    
    ## Description and Subtask    
    try:    
        description_area = main_div.find('div', {'class': 'mb-7'})
        task_description = description_area.text.strip()
        
    except:
        task_description = 'na'
        
        
    ## Subtask
    
    subtasks = []
    try:
        
        subtask_area = main_div.find('div', {'class': 'ant-table-content'})
        table_list = subtask_area.find_all('table')
        
        
        for row in table_list[0].tbody.find_all('tr'):
            for td in row.find_all('td', {'class': 'ant-table-cell w-full'}):
                subtasks.append(td.text.strip())
                
            
            
        
    except:
        subtask = 'n/a'
        subtasks.append(subtask)
    
        
        
        
    ## Scrape the activity
    
    activities = main_div.find('ul', {'class': 'ant-timeline ThreadTimeline_timeline__Gohnb'})
    bullets = activities.find_all('li')
    activity= [bullet.text for bullet in bullets]
        
        
    
    
    info = {
    'dao' : dao_name,
   'task_title': task_title,
    'status' : status,
    'priority' : priority,
    'assignee' : assignee,
    'bounty': bounty,
    'task_description': task_description,
    'activity': activity,
    'subtasks' : subtasks
    
        
}
    
    return info


## Returns the lowest scroll height of each container in the combined board
def find_bottom(grid):
    
    
    driver.execute_script("arguments[0].scrollIntoView(true);", grid)
    last_height = driver.execute_script("return arguments[0].scrollHeight", grid)
    ## Get the bottom most height of the container
    while True:
        # Scroll down to bottom
        driver.execute_script(f"arguments[0].scrollTo(0, {last_height});", grid)

        # Wait to load page
        time.sleep(5)

        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return arguments[0].scrollHeight", grid)
        if new_height == last_height:
            break

        last_height = new_height
        
    return last_height
     






## Webscraping of each url of the top dao and ingesting scraped data in to local postgres database

## table name and connection string for postgres database
table = 'dework_final'
conn_string = 'postgresql://postgres:raza@localhost:5432/wonder'


error_buttons = ['TODO:default', 'IN_PROGRESS:default', 'IN_REVIEW:default', 'DONE:processing-payment', 'DONE:paid', 'DONE:needs-payment']


error_urls= []
completed_urls = []

## for every DAO url
for url in urls:

    data = []
    try:
        ## Add board to url to get the combined board that needs to be scraped
        new_url = url + '/board'
        driver.get(new_url)

        ## Wait for the sidebar close button and click in order to remove sidebar
        close_wait = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="__next"]/section/aside/div/div/div[2]/div[2]/div/aside/div/div/div[1]/div/div[2]/button')))
        driver.find_element('xpath','//*[@id="__next"]/section/aside/div/div/div[2]/div[2]/div/aside/div/div/div[1]/div/div[2]/button').click()
        time.sleep(5)

        ## Find the 4 different containers (To do, In progress, In review and completed tasks)
        grids = driver.find_elements(By.CLASS_NAME, 'ReactVirtualized__Grid__innerScrollContainer')


        for i in range(len(grids)):
            try:

                
                completed_ids = []

                time.sleep(10)

                ## Scrolls to bottom of grid and returns the maximum possible scroll height
                last_height = find_bottom(grids[i])

                ##scrolls back to top
                driver.execute_script("arguments[0].scrollTop = 0", grids[i])
                scroll_increment = 150
                scroll_position = 0

                
                ## The unique id for every task will be appended to this list
                completed_ids = []

                ## Will keep scrolling in increments while the current scroll height is less than the maximum container scroll
                while scroll_position < last_height:
                    time.sleep(3)

                    ## Find all available rows displayed on the grid
                    elements = grids[i].find_elements(By.CSS_SELECTOR,"div[data-rbd-draggable-id")
                    for element in elements:
                        try:
                            data_rbd_draggable_id = element.get_attribute("data-rbd-draggable-id")
                            if data_rbd_draggable_id not in error_buttons and data_rbd_draggable_id not in completed_ids:
                                
                                ## Goes and clicks on the row to release pop out
                                try:
                                    row = element.find_element(By.CLASS_NAME, 'ant-card-body')
                                    wait = WebDriverWait(driver, 10)
                                    driver.execute_script("arguments[0].scrollIntoView();", row)
                                    row_click = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'ant-card-body')))
                                    row.click()



                                ## Wait for the pop up card with all the information
                                    wait = WebDriverWait(driver, 10)
                                    ant_modal_wait = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'ant-modal-body')))
                                    ant_modal = driver.find_element(By.CLASS_NAME, 'ant-modal-body')
                                    time.sleep(3)

                                    completed_ids.append(data_rbd_draggable_id)

                                ## Extract the data and put in a dictionary to add the list called data    
                                    temp = {}
                                    temp['row_id'] = data_rbd_draggable_id
                                    temp.update(extract_data())
                                    data.append(temp)


                                ## Click the close button on the pop up
                                    close_wait = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[6]/div/div[2]/div/div[2]/button')))
                                    close = driver.find_element('xpath', '/html/body/div[6]/div/div[2]/div/div[2]/button').click()









                                except ElementClickInterceptedException as e:



                                    close_wait = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[6]/div/div[2]/div/div[2]/button')))
                                    close = driver.find_element('xpath', '/html/body/div[6]/div/div[2]/div/div[2]/button')
                                    close.click()
                                    continue







                        except:
                            continue





                    time.sleep(2)

                    ## Scroll down to next element
                    scroll_position += scroll_increment    
                    driver.execute_script(f"arguments[0].scrollTo(0, {scroll_position});", grids[i])

            except:
                continue



        ## Convert the data list in to a Pandas DataFrame
        df = pd.DataFrame(data, columns=['row_id', 'dao','task_title', 'status', 'priority', 'assignee','bounty', 'task_description', 'activity', 'subtasks'])
        print(f'this {url} has been scraped')  
    

        ## Connect to the local postgres database with credentials above
        db = create_engine(conn_string)
        conn = db.connect()


        # Ingest the data in to postgres database

        df.to_sql(table, con=conn, if_exists='append',index=False)
        conn = psycopg2.connect(conn_string
                                )
        conn.autocommit = True
        cursor = conn.cursor()

        ## Close connection
        conn.close()
        completed_urls.append(url)
        print(f'{url} added to db')

            
            
    
    except:
        print(f'this {url} could not be scraped')
        error_urls.append(url)
        continue





## Print the list of urls that could not be scraped
print(error_urls)