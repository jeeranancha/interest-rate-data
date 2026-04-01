# Treasury Data Portal

Provides a clean Streamlit interface for extracting Interest Rate daily parameters from the Bank of Thailand (BOT) API and St. Louis Fed (FRED) API.

## Requirements

1. Python 3.9+
2. Valid Bank of Thailand `X-IBM-Client-Id`
3. Valid St. Louis FRED `API Key`

## Local Setup

1. Open your terminal or powershell.
2. Navigate to this folder (`cd \path\to\Mkt_IR_Input`).
3. Install dependencies by running:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the application:
   ```bash
   streamlit run app.py
   ```
5. A browser window will open automatically with the interface.

## How to Deploy to Render.com via GitHub

Render.com allows you to host Streamlit applications for free directly from a GitHub repository.

### Step 1: Uploading to GitHub
1. Create a free account on [GitHub.com](https://github.com/).
2. Create a new repository (e.g., `treasury-portal`). You can set it to **Private** for security.
3. Download and install [Git](https://git-scm.com/) on your local machine if you haven't already.
4. Open a terminal in this project's folder (`Mkt_IR_Input`) and run the following commands sequentially:
   ```bash
   git init
   git add .
   git commit -m "Initial commit of Treasury Data Portal"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/treasury-portal.git
   git push -u origin main
   ```
   *(Ensure you replace your username and repository URL).*

### Step 2: Deploying to Render.com
1. Create a free account on [Render.com](https://render.com/).
2. Click **New +** and select **Web Service**.
3. Choose **Build and deploy from a Git repository**.
4. Connect your GitHub account and select the `treasury-portal` repository you just created.
5. In the Web Service configuration:
   - **Name**: `treasury-data-portal`
   - **Environment**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `streamlit run app.py --server.port $PORT`
6. Click **Create Web Service**. Render will automatically begin building your app.
7. Wait 2-3 minutes. Once the build finishes, your app will be accessible via the generated `.onrender.com` URL!

## Data Sources
- **FRED**: SOFR, DFEDTARU, DGS1MO, DGS3MO, DGS6MO, DGS1, DGS2, DGS3, DGS5
- **BOT**: THOR_OIS 1D, THB_DISCOUNTING 1D (Policy Rate)
