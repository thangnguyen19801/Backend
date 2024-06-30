
# Project Name

## Overview

BE for analytics


## Requirements

List all the dependencies and requirements needed to run your project.

- Python 3.8
- Flask
- Flask-RESTful
- Flask-CORS
- MongoDB
- PyMongo
- Docker
- Docker Compose

## Installation

Provide step-by-step instructions on how to install and set up your project.

### Clone the Repository

\`\`\`bash
git clone [github.com/thangnguyen19801/Backend](https://github.com/thangnguyen19801/Backend.git)
cd BE
\`\`\`

### Create and Activate Virtual Environment

\`\`\`bash
python3 -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
\`\`\`

### Install Dependencies

\`\`\`bash
pip install -r requirements.txt
\`\`\`

### Setting Up Docker Compose

Ensure Docker and Docker Compose are installed on your system.

1. Start Docker.
2. Navigate to the project directory:
    \`\`\`bash
    cd yourproject
    \`\`\`
3. Run Docker Compose to set up MongoDB and Apache Spark:
    \`\`\`bash
    docker-compose up -d
    \`\`\`

### Configuration

Provide instructions on how to configure the project. For example, setting up environment variables.

### Running the Project

Explain how to run the project.

\`\`\`bash
flask run
\`\`\`

### Running with Docker

If you have a Dockerfile and want to run the Flask app using Docker, provide instructions here.

\`\`\`bash
docker build -t yourimage .
docker run -p 5000:5000 yourimage
\`\`\`