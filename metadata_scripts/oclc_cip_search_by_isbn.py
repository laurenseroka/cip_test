import requests
import csv
from ast import literal_eval
import xml.etree.ElementTree as ET
import re
from xml.dom import minidom
from fuzzywuzzy import fuzz
import timeit
import pyisbn

from dcm.dcm_helpers import *
from dcm.oclctools import *



print('Select file with oclc_rec data: ')
isbns_file = getInputFileGUI(prompt="Select file with oclc_rec data:")


print('Select output location')
output = getOutput(filename='oclc_records')

ET.register_namespace('marc', "http://www.loc.gov/MARC21/slim")

with open(isbns_file, 'r', encoding='utf-8-sig') as isbns_file:
    reader = csv.DictReader(isbns_file)
    output_fieldnames = reader.fieldnames
    output_fieldnames = output_fieldnames + [
        'oclc_count',
        'oclc_id',
        'title_match',
        'reasons_to_skip',
        'missing_fields',
        'most_holdings',
        'hld_count',
        'hld_title',
        'hld_author',
        'hld_publisher',
        'hld_date',
        
        'leader',
    ]
    add_fieldnames = []

    # List of records to print
    results = []

    for row in reader:
        oclc_ids = list()
        oclc_recs = list()
        isbn = row['isbn']

        # Search by ISBN, append ID and record to lists        
        isbn_results = 0
        #search_result_recs = oclc_sru_search_isbn(isbn)
        search_result_ids = oclc_search(isbn)
        search_result_recs = list()
        for s in search_result_ids:
            search_result_recs.append(get_oclc_record(s))

        for r in search_result_recs:
            for controlfield in r.findall('{http://www.loc.gov/MARC21/slim}controlfield'):
                if controlfield.attrib['tag'] == '001':
                    oclc_id = controlfield.text.strip()
                    if oclc_id not in oclc_ids:
                        oclc_ids.append(oclc_id)
                        oclc_recs.append(r)

        # Check each record to filter out some things, and flag others
        for record in oclc_recs:
            # list of issues we might skip
            reasons = []
            # some potentially critical missing fields
            missing_fields = []

            # Make a column for each field/subfield for review
            record_row = row.copy()
            oclc_id = ''
            record_row['leader'] = record.find('{http://www.loc.gov/MARC21/slim}leader').text
            leader_06 = record_row['leader'][6]
            for controlfield in record.findall('{http://www.loc.gov/MARC21/slim}controlfield'):
                if controlfield.attrib['tag'] not in add_fieldnames:
                    add_fieldnames.append(controlfield.attrib['tag'])
                record_row[controlfield.attrib['tag']] = controlfield.text
                if controlfield.attrib['tag'] == '001':
                    oclc_id = controlfield.text
            for datafield in record.findall('{http://www.loc.gov/MARC21/slim}datafield'):
                for subfield in datafield.findall('{http://www.loc.gov/MARC21/slim}subfield'):
                    field = str(datafield.attrib['tag']) + subfield.attrib['code']
                    if field not in add_fieldnames:
                        add_fieldnames.append(field)
                    if field in record_row and record_row[field] != '':
                        record_row[field] = record_row[field] + '|' + subfield.text
                    else:
                        record_row[field] = subfield.text
                    if field == '040b':
                        lang = subfield.text

            # Require the starting ISBN to be an 020a
            if '020a' not in record_row or isbn not in record_row['020a']:
                print('Discarded for: isbn not in 020a')
                continue

            # Require the cataloging language to be English
            lang = ''
            if '040b' in record_row:
                lang = record_row['040b']
            if lang != 'eng':
                print('Discarded for: record language not english')
                continue            

            # Fields we might want to exclude on
            if '008' not in record_row:
                missing_fields.append('008')
            if '040b' not in record_row:
                missing_fields.append('040b')
            if '300a' not in record_row:
                missing_fields.append('300a')
            if '040e' not in record_row:
                missing_fields.append('040e')
            if '050a' not in record_row or record_row['050a'] == '':
                missing_fields.append('050a')
            if '336a' not in record_row:
                missing_fields.append('336a')
            if '337a' not in record_row:
                missing_fields.append('337a')
            if '338a' not in record_row:
                missing_fields.append('338a')

            leader_17 = record_row['leader'][17]
            leader_06 = record_row['leader'][6]
            eight_23 = record_row['008'][23]

            a300 = ''
            if '300a' in record_row:
                a300 = record_row['300a']

            # Flagging issues with leader and 008
            if leader_06 != 'a':
                reasons.append('Lead6 Not a')
            if leader_17 != ' ':
                if leader_17 != 'I':
                    reasons.append('leader 17 not blank or I')
            if eight_23 != 'o':
                reasons.append('8 23 not o')


            # Flagging various issues with fields 
            if 'online' not in a300.lower():
                reasons.append('300a not online')

            if '040e' not in record_row or 'pn' not in record_row['040e']:
                reasons.append('040e not pn')
            if '040e' not in record_row or 'rda' not in record_row['040e']:
                reasons.append('040e not rda')
            
            # 264 indicator 1 - need to specifically get this since not getting indicators

            if '336a' not in record_row or 'text' not in record_row['336a']:
                reasons.append('336a not text')
            if '337a' not in record_row or 'computer' not in record_row['337a']:
                reasons.append('337a not computer')
            if '338a' not in record_row or 'online resource' not in record_row['338a'].lower():
                reasons.append('338a not online resource')


            # Flagging title matches against embedded title, google title, and openlibrary title, if they exist
            oclc_title_match_embedded = 0
            oclc_title_match_google = 0
            oclc_title_match_ol = 0

            oclc_title = ''
            title_subfields = ['245a', '245b', '245n', '245p']
            for code in title_subfields:
                if code in record_row:
                    oclc_title = (oclc_title + ' ' + record_row[code]).strip()


            if 'embedded_title' in record_row and record_row['embedded_title'] != '':
                oclc_title_match_embedded = fuzz.token_set_ratio(oclc_title, record_row['embedded_title'])
            if 'google_title' in record_row and record_row['google_title'] != '':
                oclc_title_match_google = fuzz.token_set_ratio(oclc_title, record_row['google_title'])
            if 'openlibrary_title' in record_row and record_row['openlibrary_title'] != '':
                oclc_title_match_ol = fuzz.token_set_ratio(oclc_title, record_row['openlibrary_title'])

            title_match = True
            if oclc_title_match_embedded == 0 and oclc_title_match_google ==  0 and oclc_title_match_ol == 0:
                title_match = False                    
            if (oclc_title_match_embedded  < 80):
                if oclc_title_match_google < 80 and oclc_title_match_ol < 80:
                    title_match = False


            reasons.sort()
            missing_fields.sort()
            record_row['reasons_to_skip'] = reasons
            record_row['missing_fields'] = missing_fields

            # Additional API call to get holdings data
            holdings = None
            count = 1
            while holdings == None:
                holdings = get_oclc_holdings(oclc_id)
                if holdings != None:
                    break
                count += 1
                time.sleep(10)
                if count > 5:
                    break
            hld_count = 0
            if holdings != None:
                if 'totalLibCount' in holdings:
                    hld_count = holdings['totalLibCount']
                record_row['hld_count'] = hld_count
                if 'title' in holdings:
                    record_row['hld_title'] = holdings['title']
                if 'author' in holdings:
                    record_row['hld_author'] = holdings['author']
                if 'publisher' in holdings:
                    record_row['hld_publisher'] = holdings['publisher']
                if 'date' in holdings:
                    record_row['hld_date'] = holdings['date']
            else:
                record_row['hld_count'] = -1

            isbn_results += 1
            results.append({
                'record_row': record_row.copy(),
                'isbn': isbn,
                'oclc_count': len(oclc_recs),
                'oclc_id': oclc_id,
                'title_match': title_match,
                'hld_count': hld_count,
                'most_holdings': False
            })
        print(f'Found {isbn_results} for {isbn}')


for r in results:
    isbn = r['isbn']
    holdings = r['hld_count']

    matching_isbn = [item for item in results if item['isbn'] == isbn]
    # Find the maximum 'hld_count' among matching items
    max_hld_count = max(item['hld_count'] for item in matching_isbn)
    # Check for a tie by counting occurrences of the maximum 'hld_count'
    count_of_max = sum(1 for item in matching_isbn if item['hld_count'] == max_hld_count)
    # Determine 'most_hdl' based on the tie condition
    r['most_holdings'] = 'TIE' if count_of_max > 1 else 'YES' if holdings == max_hld_count else 'NO'


add_fieldnames.sort()
remove_fields = []
final_add_fieldnames = []

for column in add_fieldnames:
    data_found = False
    for o in results:
        if column in o['record_row'] and o['record_row'][column] != '':
            data_found = True
            if column not in final_add_fieldnames:
                final_add_fieldnames.append(column)
    if data_found == False:
        remove_fields.append(column)


for r in remove_fields:
    for o in results:
        if r in o['record_row']:
            o['record_row'].pop(r)

output_fieldnames = output_fieldnames + final_add_fieldnames

with open(output, 'w', encoding='utf-8-sig') as output:
    writer = csv.DictWriter(output, fieldnames=output_fieldnames, lineterminator='\n')
    writer.writeheader()                    

    for o in results:
        resultrow = o['record_row']
        # if resultrow['bag_id'] in repeated_bags:
        #     continue
        # if o['oclc_id'] in repeated_ids:
        #     continue
        resultrow.update({
            'oclc_count': o['oclc_count'],
            'oclc_id': o['oclc_id'],
            'title_match': o['title_match'],
            'most_holdings': o['most_holdings']
        })
        writer.writerow(resultrow)