#Today I am learning about string maipluation in python
# 
# name = "Baraka Mnjala Mbugua"
# 
# first_name = name[0:6]
# second_name = name[6:13]
# last_name = name[13:20]
# 
# print(first_name)
# print(second_name)
# print(last_name)
# 
# website = "https://www.barakambuguaon.top"
#print the coola part of the website
# cool_slice = slice(12,-4)
# print(website[cool_slice])
# 
#function to take all the individual parts of the domain
def seperator (url):
    if url[:5] != "https":
        print("Protocol not added ie https://")
        return
    protocol = url[:5]
    domain = url[8:-4]
    extension = url[-4:]
    print(f"The protocol is {protocol}, the domain is {domain}, and the extension is {extension}")

user_url = input("Enter the url: ")
seperator(user_url)