#-------------------------------------------------------------------------------
# Name:        Clean Google Places json file
# Purpose:     This program serves a number of purposes. It takes in corrupted
#              json (places.original.json) and makes it valid. It filters the 
#              data to match the data in the primary Hotel_Reviews.csv data set.
#              It adds additional fields based on the existing fields, and
#              denormalises fields which cannot be read into Spark/pandas even
#              if the json is valid (json nested arrays).
#
# Author:      Ashwath Sampath
#
# Created:     14-06-2020
# Copyright:   (c) Ashwath Sampath 2020
#-------------------------------------------------------------------------------

import collections
import json
import jsonlines
import ast
from tqdm import tqdm
from geotext import GeoText

def flatten(day_hours_list):
    """ Generator which flattens a list of lists recursively by yielding strings and ints/floats
    directly and recursively calling the generator func if it's an iterable """
    for el in day_hours_list:
        if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el

def process_hours(opening_hours):
    """ Takes a Json array of opening_hours in the following form (None if not present), and
    returns a dictionary containing days as keys and opening hours as string values
    [['Monday', [['6:30 am--4:15 pm']]],
    ['Tuesday', [['6:30 am--4:15 pm']]],
    ['Wednesday', [['6:30 am--4:15 pm']], 1],
    ['Thursday', [['6:30 am--4:15 pm']]],
    ['Friday', [['6:30 am--4:15 pm']]],
    ['Saturday', [['6:30 am--4:15 pm']]],
    ['Sunday', [['6:30 am--4:15 pm']]]] """
    day_keys = ['MondayHours', 'TuesdayHours', 'WednesdayHours', 'ThursdayHours',
            'FridayHours', 'SaturdayHours', 'SundayHours']
    if opening_hours is None:
        # Return None for each day
        return {k:None for k in day_keys}
    return_dict = dict()
    for day_hours in opening_hours:
        # day_hours[0] is the day, [1] is the list of lists
        return_dict[day_hours[0]+'Hours'] = next(flatten(day_hours[1]))
        
    return return_dict   


def main():
    """ Main function which reads corrupted json provided by the original creators of the data set, 
    cleans it, and also adds new fields as required. Only records for 6 countries (corresponding to
    the countries in which there are hotels in Hotel_Reviews.csv) are retained."""
    # The following 6 countries have hotels in the Hotel_Reviews.csv data set. This was found while
    # doing an exploratory analysis of that data set.
    countries = ['france', 'italy', 'spain', 'united kingdom', 'austria', 'netherlands']
    with jsonlines.open('../Data/Cleaned/google_places_cleaned.jsonl', mode='w') as writer:
        with open('../Data/Original/places.original.json', 'r') as testfile:
            # fields: 'name', 'price', 'address', 'hours', 'phone', 'closed', 'gPlusPlaceId', 'gps'
            for line in tqdm(testfile):
                normalised_dict = ast.literal_eval(line)
                joined_address = ', '.join(normalised_dict.get('address'))
                if any(country in joined_address.lower() for country in countries):
                    # Country found in the list, find which country it is.
                    # GeoText module only detects the city/country if it is capitalised
                    geo_address = GeoText(joined_address.title())
                    try:
                        matching_country = geo_address.countries[0]
                    except IndexError:
                        matching_country = [country for country in countries if country in joined_address.lower()][0].title()
                    try:
                        #If it matches 2 cities for some reason, take the one closer to the end of the string.
                        matching_city = geo_address.cities[-1]
                    except IndexError:
                        matching_city = None
                    normalised_dict['country'] = matching_country
                    normalised_dict['city'] = matching_city
                    try:
                        # We get a TypeError if gps is None. These records can be discarded as we will use
                        # latitude and longitude to join the googl places and Hotel reviews files.
                        normalised_dict['latitude'] = normalised_dict.get('gps')[0]
                        normalised_dict['longitude'] = normalised_dict.get('gps')[1]
                    except TypeError:
                        continue
                    opening_hours_dict = process_hours(normalised_dict.get('hours'))
                    # Add the new keys (Monday, Tuesday, ...)
                    normalised_dict.update(opening_hours_dict)
                    del normalised_dict['closed']
                    del normalised_dict['gps']
                    del normalised_dict['hours']

                    # Write to output jsonl file
                    writer.write(normalised_dict)

    
if __name__ == '__main__':
    main()
