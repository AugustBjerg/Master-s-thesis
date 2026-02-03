# Connecting Local VSCode to UCloud Coder via SSH
This guide explains how to connect your local VSCode installation to a UCloud Coder instance using SSH, allowing you to develop on UCloud's computing resources while using your local VSCode interface.

Prerequisites
Windows PowerShell (Administrator access)



Visual Studio Code installed locally

UCloud account with access to Coder application

Step 1: Generate SSH Key on Local Machine
Open PowerShell as Administrator and run:

powershell
# Enable the SSH agent service
Set-Service ssh-agent -StartupType Automatic

# Start the service
Start-Service ssh-agent

# Verify it's running
Get-Service ssh-agent

# Generate SSH key
ssh-keygen -t ed25519 -C "your_email@example.com"
When prompted:

Press Enter to save in the default location (C:\Users\YourUsername\.ssh\id_ed25519)

Enter a passphrase (optional but recommended)

Confirm the passphrase

Step 2: Add SSH Key to Agent
powershell
# Add your private key to the SSH agent
ssh-add $env:USERPROFILE\.ssh\id_ed25519

# Copy public key to clipboard
Get-Content $env:USERPROFILE\.ssh\id_ed25519.pub | clip
Step 3: Add Public Key to UCloud
Go to UCloud web interface

Navigate to Resources → SSH Keys

Click "Add SSH Key"

Paste your public key (already in clipboard from previous step)

Give it a descriptive name (e.g., "My Laptop")

Click Save

Step 4: Launch Coder with SSH Enabled
In UCloud, go to Apps → Coder

Configure machine type and mount your project folders

Important: Check the box "Enable SSH server"

Click Submit to start the job

Step 5: Get SSH Connection Details
Once the Coder job starts:

Click on the running job in the Jobs list

Click the SSH tab in the job interface

Note the SSH command displayed, which looks like:

text
ssh ucloud@ssh.cloud.sdu.dk -p XXXX
Write down the port number (e.g., 2696) - this changes every time you start a new job

Step 6: Configure VSCode Remote-SSH
Install Remote-SSH Extension
Open VSCode on your local machine

Open Extensions (Ctrl+Shift+X)

Search for "Remote - SSH"

Install the extension from Microsoft

Configure SSH Connection
Press Ctrl+Shift+P

Type: "Remote-SSH: Open SSH Configuration File"

Select your config file (usually C:\Users\YourUsername\.ssh\config)

Add the following configuration:

text
Host ucloud-coder
    HostName ssh.cloud.sdu.dk
    User ucloud
    Port XXXX
    IdentityFile ~/.ssh/id_ed25519
Important notes:

Replace XXXX with your actual port number from Step 5

The hostname is ssh.cloud.sdu.dk (NOT ssh.ucloud.sdu.dk)

The port number changes each time you start a new Coder job

Save the file

Step 7: Connect to UCloud
Press Ctrl+Shift+P

Type: "Remote-SSH: Connect to Host"

Select "ucloud-coder"

Enter your SSH key passphrase if prompted

Select "Linux" as the platform if asked

Wait for the connection to establish

Step 8: Open Your Project
Once connected:

Click File → Open Folder

Navigate to /work/YourProjectName/

Click OK

You're now working on UCloud's computing resources using your local VSCode!