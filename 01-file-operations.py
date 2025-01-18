# Exercise: Quick Student Profile with pathlib
# Time: 5 minutes

from pathlib import Path
import json


def quick_profile_update():
    """
    Exercise: Create and update a simple student profile

    Tasks (4 minutes total):
    1. Create a profiles directory
    2. Save your basic profile info
    3. List all json files in profiles directory
    4. Print your profile contents
    """
    # 1. Create profiles directory
    # TODO: Create a directory called 'profiles' using pathlib

    # 2. Create and save your profile
    my_profile = {
        "name": "Your Name",
        "languages": ["Python"],  # Add more if you know more!
        "favorite_hobby": "",
    }
    # TODO: Save my_profile as 'profiles/my_profile.json'

    # 3. List all profiles
    # TODO: Use Path().glob() to find all .json files in profiles directory

    # 4. Read and print your profile
    # TODO: Read and print the contents of your profile file


if __name__ == "__main__":
    pass
    # TODO: Call quick_profile_update()
