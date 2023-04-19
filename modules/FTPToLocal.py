import ftplib

# Connect to the FTP server
ftp = ftplib.FTP('ftp.ebi.ac.uk')
ftp.login(user='anonymous', passwd='youremail@example.com')

# Change to the directory containing the file to download
ftp.cwd('pub/databases/opentargets/platform/23.02/output/etl/json/interactionEvidence/')

# Download the file
filename = '/Users/cheny39/Documents/tmp/part-00342-9b1f55fa-7c40-42c2-838f-3c0b0c922551-c000.json'
localfile = open(filename, 'wb')
ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
localfile.close()

# Close the connection to the FTP server
ftp.quit()