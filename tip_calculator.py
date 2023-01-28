print("Welcome to the tip calculator!")
bill = float(input("What was the total bill? $"))
percent = float(input("How much tip would you like to give? 10, 12, or 15? "))
people = float(input("How many people to split the bill? "))

pay = ((bill * (percent/100))+bill)/people

print("Each person should pay: " + "{:.2f}".format(pay))