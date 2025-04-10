# Comparis Public Airflow Plugins

This is mirror of some of the operators and hooks that we have developed for our internal use, 
hoping it would be useful for the rest of the community.

## Local Development (uv)

Create the virutal environment and install all dependencies:
```
uv python install 3.11
```

Then, activate the virtual environment:
```
uv sync
```

## How to use

Place the plugins in the `plugins` folder of your Airflow installation. The plugins will be automatically loaded by Airflow. 
You can use symlink or copy the files directly into the `plugins` folder.

