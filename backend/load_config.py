import os
import json
from dotenv import load_dotenv

def load_connectors_config():
    # Load environment variables from .env file
    load_dotenv()
    
    # Read the template
    with open('connectors.template.json', 'r') as f:
        template = f.read()
    
    # Replace environment variables
    config_str = os.path.expandvars(template)
    
    # Parse the JSON
    config = json.loads(config_str)
    
    # Write to connectors.local.json (ignored by git)
    with open('connectors.local.json', 'w') as f:
        json.dump(config, f, indent=2)
    
    return config

if __name__ == "__main__":
    load_connectors_config()
