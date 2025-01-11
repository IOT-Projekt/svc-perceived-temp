def calculate_perceived_temperature(temperature_celsius: float, humidity: float) -> float:
    """
    Calculate the perceived temperature based on the real temperature and humidity.
    
    :param temperature_celsius: Real temperature in Celsius
    :param humidity: Humidity percentage (0-100)
    :return: Perceived temperature in Celsius
    """
    # Constants based on: https://en.wikipedia.org/wiki/Heat_index
    C1 = -8.78469475556
    C2 = 1.61139411
    C3 = 2.33854883889
    C4 = -0.14611605
    C5 = -0.012308094
    C6 = -0.0164248277778
    C7 = 0.002211732
    C8 = 0.00072546
    C9 = -0.000003582
    
    # Calculate the perceived temp with the heat index heat index
    heat_index = ( C1 
                  + C2 * temperature_celsius 
                  + C3 * humidity 
                  + C4 * temperature_celsius * humidity 
                  + C5 * temperature_celsius**2 
                  + C6 * humidity**2 
                  + C7 * temperature_celsius**2 * humidity 
                  + C8 * temperature_celsius * humidity**2 
                  + C9 * temperature_celsius**2 * humidity**2 )

    
    return heat_index