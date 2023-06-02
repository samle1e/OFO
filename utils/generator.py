import os
import toml

def create_secrets():
    # Read the example secrets file
    example_secrets = toml.load('.streamlit/secrets.example.toml')

    # Create a dictionary to store the secrets
    secrets = {}

    # Loop through the keys in the example secrets file
    for parent_key, parent_value in example_secrets.items():
        # Check if the value is a nested object
        if isinstance(parent_value, dict):
            # Loop through the nested keys
            for nested_key, nested_value in parent_value.items():
                # Construct the environment variable name
                env_var_name = f'sbdh_{nested_key}'

                # Retrieve the value from the host environment
                nested_env_value = os.getenv(env_var_name)

                # If the environment variable exists, add it to the secrets dictionary
                if nested_env_value is not None:
                    secrets.setdefault(parent_key, {})[nested_key] = nested_env_value

    # # Check if the secrets.toml file exists
    # if os.path.exists('.streamlit/secrets.toml'):
    #     # If it exists, remove it
    #     os.remove('.streamlit/secrets.toml')

    # Write the secrets to the secrets.toml file
    with open('.streamlit/secrets.toml', 'w') as f:
        toml.dump(secrets, f)

if __name__ == '__main__':
    create_secrets()
