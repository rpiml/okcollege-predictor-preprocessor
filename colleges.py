import redis
import time

def get_college_index():
    print("Getting college_index")
    college_bytes = open("./assets/colleges.csv").read()
    college_lines = str(college_bytes).split("\n")
    colleges = [line.split("\t")[0] for line in college_lines]
    college_dict = {}
    for i,name in enumerate(colleges):
        college_dict[i] = name
    return college_dict
