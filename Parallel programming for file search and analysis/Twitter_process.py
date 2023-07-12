import pandas as pd
import collections
import re
import time
import os
from mpi4py import MPI
from tabulate import tabulate
import sys


# initialise MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Get the current directory of the script
current_dir = str(os.path.dirname(os.path.abspath(__file__)))

filename = sys.argv[1]
# sys.argv[1]
# "/smallTwitter.json"
# "/twitter-data-small.json"

def split_list(input_list, n):
    chunk_size = (len(input_list) + n - 1) // n
    return [input_list[i:i+chunk_size] for i in range(0, len(input_list), chunk_size)]

# check each step's time
read_data_start_time = time.time()

authors = []
locations = []


with open(current_dir+"/"+filename, "r", encoding='UTF-8') as f:
    for line in f:
        # check for author ID
        if "author_id" in line:
            # author id takes 20 places before the actural id
            match = re.search(r':\s*"(\d+)"\s*,', line)
            if match:
                authors.append(match.group(1))
        elif "full_name" in line:
            match = re.search(r':\s*"([^"]+)"\s*,', line)
            if match:
                locations.append(match.group(1))
                
sal = pd.read_json(current_dir+"/sal.json")


read_data_end_time = time.time()
read_data_run_time = read_data_end_time - read_data_start_time

locations = split_list(locations, size)
authors = split_list(authors, size)
locations = comm.scatter(locations, root=0)
authors = comm.scatter(authors, root=0)

# filter sal so gcc only contains metropolitan suburbs
gmetro = ["1gsyd", "2gmel", "3gbri", "4gade", "5gper", "6ghob", "7gdar", "8acte", "9oter"]
states = ["new south wales", "victoria", "queensland", "south australia", "western australia",\
 "tasmania", "northern territory", "australian capital territory", "other territory"]
capitals_name = {"1gsyd" : "Greater Sydney",
                 "2gmel" : "Greater Melbourne",
                 "3gbri" : "Greater Brisbane",
                 "4gade" : "Greater Adelaide",
                 "5gper" : "Greater Perth",
                 "6ghob" : "Greater Hobart",
                 "7gdar" : "Greater Darwin",
                 "8acte" : "Greater Canberra",
                 "9oter" : "Other Territory"
}
                 
# filter sal so dataframe only contains greater capital gccs
# seperate dict into different gccs
metro = collections.defaultdict()
for gcc in gmetro:
    gcc_dict = sal[sal.columns[sal.isin([gcc]).any()]]
    metro[gcc] = gcc_dict
    

# collect only greater capital city from sal
sal = sal[sal.columns[sal.isin(gmetro).any()]]


high_freq_suburb = {"central coast" : "1gsyd",
    "sydney" : "1gsyd", 
    "melbourne": "2gmel", 
    "brisbane" : "3gbri", 
    "adelaide" : "4gade",
    "perth" : "5gper", 
    "hobart" : "6ghob", 
    "darwin" : "7gdar", 
    "canberra" : "8acte"}


# retrieve gcc of tweet
def retrieve_gcc(tweet_data, state_name, greater_labels, greater_sal, sal):

    location = tweet_data.lower().split(", ")
    # position is the position of the word in location
    gcc = 0
    first_location = location[0].lower()
    # if location is "Australia" or state name, can't get suburb, skip
    if (first_location == 'australia') or (first_location in states):
        return gcc
    
    if first_location in high_freq_suburb:
        gcc = high_freq_suburb[first_location]
        return gcc
    # valid location containing states, check for suburb info
    contain_state = False
    
    # go through the locations and get the state
    for j in range(len(location)):
        # state not first in location, it contains suburb
        if location[j] in state_name and j != 0:
            gcc = greater_labels[state_name.index(location[j])]
            contain_state = True
            suburbs = list(greater_sal[gcc])
            break

    # find suburb in metro
    if contain_state == False:
        suburbs = list(sal)
        
    # suburb should be the first of location
    # there are situation of Torquay - Jan Juc, take Jan Juc
    suburb = location[0].split(' - ')[-1]
    
    # search for suburbs in sal
    r = re.compile(f"^{suburb}$|^{suburb} \(.*\)")
    possible_subs = list(filter(r.match, suburbs))

    # if there are no matched suburb, return 0
    if len(possible_subs) == 0:
        return 0
    
    # if there are possible suburbs, and we know the state, return state's gcc
    elif len(possible_subs) >= 1 and contain_state:
        return gcc
    
    # if we found multiple suburbs containing tweet suburb name but don't know the state
    else:
        # gcc is 0 in this case
        # go through all possible suburbs and check if gcc is the same, give a temperary gcc
        temp_gcc = sal[possible_subs[0]].gcc
        for possible_sub in possible_subs:
            # when gcc does not match, we do not know which gcc this tweet is from, return 0 straight away
            if sal[possible_sub].gcc != temp_gcc:
                return 0
        return temp_gcc






def print_location_count(greater_dict):
    # Sort the list of tuples in descending order of the count
    sorted_list = sorted(greater_dict.items(), key=lambda x: x[1], reverse=True)
    
    # Print the table
    print(tabulate([[f"{gcc}({capitals_name[gcc]})",count] for gcc, count in sorted_list], headers=["Greater Capital City","Number of Tweets Made"]), "\n")


def print_author_top10_tweet_count(top10_data):
    length = [len(author[0]) for author in top10_data if len(author[0]) > 0]
    author_width = max(length)
    rank_width = 4
    tweets_width = author_width + 1
    # Define the table format string
    table_format = "{:<{rank_width}} | {:<{author_width}} | {:<{tweets_width}}"
    # Print the table header
    dash_length = rank_width+author_width+len("Number of Tweets Made")+6
    print(table_format.format("Rank", "Author ID", "Number of Tweets Made", \
                              rank_width=rank_width, author_width=author_width, tweets_width=tweets_width))
    print("-" * dash_length)

    rank = 1
    # use temp_tweet count to check if current author tweeted the same number of tweets as the previous author
    temp_tweet_count = 0
    # count is number of author with same number of tweets
    count = 1
    # used to print author's rank when they have number of same tweets
    same_count_rank = 0
    
    # Print each row of the table using format
    for row in top10_data:
        # retreive author id and number of tweets
        author = row[0]
        tweet_count = row[1]
        
        # print out first 10 first
        if rank <= 10:
            # if two author has the same count of tweets, they should rank the same
            if tweet_count == temp_tweet_count:
                # their rank would be the same as previous author's
                same_count_rank = rank - count
                #
                count += 1
                print(table_format.format("#"+str(same_count_rank), author, tweet_count, \
                                          rank_width=rank_width, author_width=author_width, tweets_width=tweets_width))
                temp_tweet_count = tweet_count
            # tweet count different, new rank for this author, return count to 1
            else:
                count = 1
                print(table_format.format("#"+str(rank), author, tweet_count, \
                                          rank_width=rank_width, author_width=author_width, tweets_width=tweets_width))
                temp_tweet_count = tweet_count
            rank += 1
        # check if author after first 10 has the same number of tweet count, print if match
        elif rank > 10 and tweet_count == temp_tweet_count:
            # rank would be the same as previous author's
            same_count_rank = rank - count
            count += 1
            print(table_format.format("#"+str(same_count_rank), author, tweet_count, \
                                  rank_width=rank_width, author_width=author_width, tweets_width=tweets_width))
            rank += 1
            # no update on temp_tweet_count, only author with tweet count equal to 10th is print
        else:
            break
    print("\n")


def print_author_top10_gcc_count(top10_data, author_dict):

    length = [len(author[0]) for author in top10_data if len(author[0]) > 0]
    author_width = max(length)
    rank_width = 4
    tweets_width = author_width + 1

    # Define the table format string
    table_format = "{:<{rank_width}} | {:<{author_width}} | {:<{tweets_width}}"
    # Print the table header
    dash_length = rank_width+author_width+len("Number of Unique City Locations and #Tweets")+6
    print(table_format.format("Rank", "Author ID", "Number of Unique City Locations and #Tweets", \
                              rank_width=rank_width, author_width=author_width, tweets_width=tweets_width))
    print("-" * dash_length)
    
    rank = 1
    temp_tweet_count = 0
    temp_city_count = 0
    
    count = 1
    # used to print author's rank when they have number of same tweets
    same_count_rank = 0
    
    # Print each row of the table using format
    for row in top10_data:
        author = row[0]
        num_cities = str(len(row[1]))
        num_of_tweets = str(sum(author_dict[author].values()))
        
        # make sure first ten is print out
        if rank <= 10:
            city_list = ""
            for key, values in author_dict[author].items():
                if len(city_list) != 0:
                    city_list+= ", "
                city_list += str(values)+str(key[1:])
            city_list += ")"
            
            # if num of city and num of tweet equal to the previous one, they have the same rank
            if num_cities == temp_city_count and num_of_tweets == temp_tweet_count:
                
                same_count_rank = rank - count
                string = num_cities + " (#" + num_of_tweets +" tweets - "+city_list

                print(table_format.format("#"+str(same_count_rank), author, string, \
                                          rank_width=rank_width, author_width=author_width, tweets_width=tweets_width))
                rank += 1
                count += 1
            else:
                string = num_cities + " (#" + num_of_tweets +" tweets - "+city_list

                print(table_format.format("#"+str(rank), author, string, \
                                          rank_width=rank_width, author_width=author_width, tweets_width=tweets_width))
                rank += 1
                count = 1
                temp_city_count = num_cities
                temp_tweet_count = num_of_tweets
        elif rank > 10 and num_cities == temp_city_count and num_of_tweets == temp_tweet_count:
            same_count_rank = rank - count
            string = num_cities + " (#" + num_of_tweets +" tweets - "+city_list

            print(table_format.format("#"+str(same_count_rank), author, string, \
                                      rank_width=rank_width, author_width=author_width, tweets_width=tweets_width))
            rank += 1
            count += 1
        else:
            break
    print("\n")
        

# initiate start time for processing
process_start_time = time.time()

# create an empty dictionary to add author id and number of tweets in
author_dict = collections.defaultdict(int)
greater_dict = dict.fromkeys(gmetro, 0)
all_author_tweets = collections.defaultdict(int)

for i in range(len(authors)):
    # retrieve author id of this tweet
    author_id = authors[i]
    
    # create a dictionary containing counts of individual author's tweets
    all_author_tweets[author_id] = all_author_tweets.get(author_id,0) + 1
    
    # retrieve location information about this tweet
    location_info = locations[i]
    gcc = retrieve_gcc(location_info, states, gmetro, metro, sal)
    
    # if gcc is not 0, then this is tweet is tweeted in greater Metropolitan area
    # add in dictionary
    if gcc == 0:
        continue
    # add up greater area
    greater_dict[gcc] += 1
    # check if author id exist in dictionary
    if author_id not in author_dict:
        location_dict = {}
        # create a location dict that contains gcc as key
        location_dict[gcc] = location_dict.get(gcc,0) + 1
        # create new author
        author_dict[author_id] = location_dict
        
    # author id is found in the author id
    else:
        # use get(gcc,0) to create new gcc if gcc is not present at current author's dictionary
        author_dict[author_id][gcc] = author_dict[author_id].get(gcc,0) + 1

    
# gather all results across all processes
greater_dicts = comm.gather(greater_dict, root=0)
author_dicts = comm.gather(author_dict, root=0)
all_author_tweets_dict = comm.gather(all_author_tweets, root=0)


# create empty dictionary to merge gathered dictionary, as they are in dimension of 4 * chunksize
merged_author_dict = collections.defaultdict(int)
merged_greater_dict = collections.defaultdict(int)
merged_all_authors = collections.defaultdict(int)

# Merge author_dicts from all processes into one
if rank == 0:
    for author_dict in author_dicts:
        for author_id, location_dict in author_dict.items():
            if author_id not in merged_author_dict:
                merged_author_dict[author_id] = {}
            for gcc, count in location_dict.items():
                merged_author_dict[author_id][gcc] = merged_author_dict[author_id].get(gcc, 0) + count

    for sub_dict in greater_dicts:
        for gcc, count in sub_dict.items():
            merged_greater_dict[gcc] = merged_greater_dict[gcc] + count
            
    for authors in all_author_tweets_dict:
        for author_id, count in authors.items():
            merged_all_authors[author_id] = merged_all_authors[author_id] + count


process_end_time = time.time()
process_runtime = process_end_time - process_start_time


if rank == 0:
    # sort the dictionary by the sum of key values
    # sum_tweet = {key: sum(values) for key, values in merged_all_authors.items()}
    sorted_author = sorted(merged_all_authors.items(), key=lambda x: x[1], reverse=True)

    # sort the dictionary by the sum of key values
    sorted_author_gcc = sorted(merged_author_dict.items(), key=lambda x: (len(x[1]), sum(x[1].values()), ), reverse=True)

    # print count of tweets in greater capital city
    print_location_count(merged_greater_dict)
    
    # print top 10 author that made most tweets from all Australia
    print_author_top10_tweet_count(sorted_author)
    
    # print top 10 author that made most tweets in differenty greater capital city
    print_author_top10_gcc_count(sorted_author_gcc, merged_author_dict)

    end_time = time.time()
    total_run_time = end_time - read_data_start_time
    
    print(f"Run time of {size} cores are:")
    print(f"Runtime of the reading data: {read_data_run_time} seconds")
    print(f"Runtime of the process data: {process_runtime} seconds")
    print(f"Runtime of the script: {total_run_time} seconds")
