#Today I am learning about string maipluation in python

name = "Baraka Mnjala Mbugua"

first_name = name[0:6]
second_name = name[6:13]
last_name = name[13:20]

print(first_name)
print(second_name)
print(last_name)

website = "https://www.barakambuguaon.top"
#print the coola part of the website
cool_slice = slice(12,-4)
print(website[cool_slice])