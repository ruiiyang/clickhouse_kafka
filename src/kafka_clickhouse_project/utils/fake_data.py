from typing import Dict, Any
import random
import json
import uuid
# Function to generate fake data
def generate_fake_data(num_messages=1):
    """
    Generate fake photo data with tags in JSON format.

    Args:
        num_messages (int): Number of photo messages to generate. to simplify, it is 1. But in theory, it should be randome number of message that is being generated per second. 

    Returns:
        list: A list of dictionaries, each representing a photo with tags.
    """
    all_photos = []
    
    for _ in range(num_messages):
        tags = []
        for _ in range(2):  # Generate exactly 2 tags per photo
            tags.append({
                "id": str(uuid.uuid4()),  # Generate a unique ID for each tag
                "raw": f"tag_{random.randint(1, 100)}"  # Generate a random tag name
            })
        
        # Construct the photo JSON structure
        photo_json = {
            "photo": {
                "tags": {
                    "tag": tags  # Add the list of tags
                }
            }
        }
        all_photos.append(photo_json)
    
    return all_photos