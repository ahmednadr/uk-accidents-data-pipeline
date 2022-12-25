import math


def Marc (bf0, n, PHI0, PHI):
    return bf0 * (((1 + n + ((5 / 4) * math.pow(n, 2)) + ((5 / 4) * math.pow(n, 3))) * (PHI - PHI0)) - (((3 * n) + (3 * math.pow(n, 2)) + ((21 / 8) * math.pow(n, 3))) * (math.sin(PHI - PHI0)) * (math.cos(PHI + PHI0))) + ((((15 / 8) * math.pow(n, 2)) + ((15 / 8) * math.pow(n, 3))) * (math.sin(2 * (PHI - PHI0))) * (math.cos(2 * (PHI + PHI0)))) - (((35 / 24) * math.pow(n, 3)) * (math.sin(3 * (PHI - PHI0))) * (math.cos(3 * (PHI + PHI0)))))


def InitialLat (North, n0, afo, PHI0, n, bfo):

    PHI1 = ((North - n0) / afo) + PHI0
    M = Marc(bfo, n, PHI0, PHI1)
    PHI2 = ((North - n0 - M) / afo) + PHI1
    while (abs(North - n0 - M) > 0.00001):
        PHI2 = ((North - n0 - M) / afo) + PHI1
        M = Marc(bfo, n, PHI0, PHI2)
        PHI1 = PHI2
    return PHI2

def E_N_to_Lat (East, North, a, b, e0, n0, f0, PHI0, LAM0):
    Pi = 3.14159265358979
    RadPHI0 = PHI0 * (Pi / 180)
    RadLAM0 = LAM0 * (Pi / 180)
    af0 = a * f0
    bf0 = b * f0
    e2 = (math.pow(af0, 2) - math.pow(bf0, 2)) / math.pow(af0, 2)
    n = (af0 - bf0) / (af0 + bf0)
    Et = East - e0
    PHId = InitialLat(North, n0, af0, RadPHI0, n, bf0)
    nu = af0 / (math.sqrt(1 - (e2 * (math.pow(math.sin(PHId), 2)))))
    rho = (nu * (1 - e2)) / (1 - (e2 * math.pow(math.sin(PHId), 2)))
    eta2 = (nu / rho) - 1
    VII = (math.tan(PHId)) / (2 * rho * nu)
    VIII = ((math.tan(PHId)) / (24 * rho * math.pow(nu, 3))) * (5 + (3 * (math.pow(math.tan(PHId), 2))) + eta2 - (9 * eta2 * (math.pow(math.tan(PHId), 2))))
    IX = ((math.tan(PHId)) / (720 * rho * math.pow(nu, 5))) * (61 + (90 * ((math.tan(PHId)) ** 2)) + (45 * (math.pow(math.tan(PHId), 4))))
    E_N_to_Lat = (180 / Pi) * (PHId - (math.pow(Et, 2) * VII) + (math.pow(Et, 4) * VIII) - ((Et ** 6) * IX))
    return ( E_N_to_Lat) 


def E_N_to_Long (East, North, a, b, e0, n0, f0, PHI0, LAM0):

    Pi = 3.14159265358979
    RadPHI0 = PHI0 * (Pi / 180)
    RadLAM0 = LAM0 * (Pi / 180)
    af0 = a * f0
    bf0 = b * f0
    e2 = (math.pow(af0, 2) - math.pow(bf0, 2)) / math.pow(af0, 2)
    n = (af0 - bf0) / (af0 + bf0)
    Et = East - e0
    PHId = InitialLat(North, n0, af0, RadPHI0, n, bf0)
    nu = af0 / (math.sqrt(1 - (e2 * (math.pow(math.sin(PHId), 2)))))
    rho = (nu * (1 - e2)) / (1 - (e2 * math.pow(math.sin(PHId), 2)))
    eta2 = (nu / rho) - 1
    X = (math.pow(math.cos(PHId), -1)) / nu
    XI = ((math.pow(math.cos(PHId), -1)) / (6 * math.pow(nu, 3))) * ((nu / rho) + (2 * (math.pow(math.tan(PHId), 2))))
    XII = ((math.pow(math.cos(PHId), -1)) / (120 * math.pow(nu, 5))) * (5 + (28 * (math.pow(math.tan(PHId), 2))) + (24 * (math.pow(math.tan(PHId), 4))))
    XIIA = ((math.pow(math.cos(PHId), -1)) / (5040 * math.pow(nu, 7))) * (61 + (662 * (math.pow(math.tan(PHId), 2))) + (1320 * (math.pow(math.tan(PHId), 4))) + (720 * (math.pow(math.tan(PHId), 6))))
    E_N_to_Long = (180 / Pi) * (RadLAM0 + (Et * X) - (math.pow(Et, 3) * XI) + (math.pow(Et, 5) * XII) - (math.pow(Et, 7) * XIIA))
    return E_N_to_Long

def Lat_Long_H_to_X (PHI, LAM, H, a, b):


    Pi = 3.14159265358979
    RadPHI = PHI * (Pi / 180)
    RadLAM = LAM * (Pi / 180)
    e2 = (math.pow(a, 2) - math.pow(b, 2)) / math.pow(a, 2)
    V = a / (math.sqrt(1 - (e2 * (math.pow(math.sin(RadPHI), 2)))))
    return (V + H) * (math.cos(RadPHI)) * (math.cos(RadLAM))

def Lat_Long_H_to_Y (PHI, LAM, H, a, b):


    Pi = 3.14159265358979
    RadPHI = PHI * (Pi / 180)
    RadLAM = LAM * (Pi / 180)
    e2 = (math.pow(a, 2) - math.pow(b, 2)) / math.pow(a, 2)
    V = a / (math.sqrt(1 - (e2 * (math.pow(math.sin(RadPHI), 2)))))
    return (V + H) * (math.cos(RadPHI)) * (math.sin(RadLAM))

def Lat_H_to_Z (PHI, H, a, b):

    Pi = 3.14159265358979
    RadPHI = PHI * (Pi / 180)
    e2 = (math.pow(a, 2) - math.pow(b, 2)) / math.pow(a, 2)
    V = a / (math.sqrt(1 - (e2 * (math.pow(math.sin(RadPHI), 2)))))
    return ((V * (1 - e2)) + H) * (math.sin(RadPHI))

def Helmert_X (X, Y, Z, DX, Y_Rot, Z_Rot, s):


    Pi = 3.14159265358979
    sfactor = s * 0.000001
    RadY_Rot = (Y_Rot / 3600) * (Pi / 180)
    RadZ_Rot = (Z_Rot / 3600) * (Pi / 180)
    return ( X + (X * sfactor) - (Y * RadZ_Rot) + (Z * RadY_Rot) + DX) 

def Helmert_Y (X, Y, Z, DY, X_Rot, Z_Rot, s):


    Pi = 3.14159265358979
    sfactor = s * 0.000001
    RadX_Rot = (X_Rot / 3600) * (Pi / 180)
    RadZ_Rot = (Z_Rot / 3600) * (Pi / 180)
    return (X * RadZ_Rot) + Y + (Y * sfactor) - (Z * RadX_Rot) + DY

def Helmert_Z (X, Y, Z, DZ, X_Rot, Y_Rot, s):

    Pi = 3.14159265358979
    sfactor = s * 0.000001
    RadX_Rot = (X_Rot / 3600) * (Pi / 180)
    RadY_Rot = (Y_Rot / 3600) * (Pi / 180)
    return (-1 * X * RadY_Rot) + (Y * RadX_Rot) + Z + (Z * sfactor) + DZ



def Iterate_XYZ_to_Lat (a, e2, PHI1, Z, RootXYSqr):


    V = a / (math.sqrt(1 - (e2 * math.pow(math.sin(PHI1), 2))))
    PHI2 = math.atan2((Z + (e2 * V * (math.sin(PHI1)))), RootXYSqr)
    while (abs(PHI1 - PHI2) > 0.000000001) :
        PHI1 = PHI2
        V = a / (math.sqrt(1 - (e2 * math.pow(math.sin(PHI1), 2))))
        PHI2 = math.atan2((Z + (e2 * V * (math.sin(PHI1)))), RootXYSqr)
    
    return PHI2

def XYZ_to_Lat (X, Y, Z, a, b):

    RootXYSqr = math.sqrt(math.pow(X, 2) + math.pow(Y, 2))
    e2 = (math.pow(a, 2) - math.pow(b, 2)) / math.pow(a, 2)
    PHI1 = math.atan2(Z, (RootXYSqr * (1 - e2)))
    PHI = Iterate_XYZ_to_Lat(a, e2, PHI1, Z, RootXYSqr)
    Pi = 3.14159265358979
    return PHI * (180 / Pi)

def XYZ_to_Long (X, Y):
    Pi = 3.14159265358979
    return math.atan2(Y, X) * (180 / Pi)

def convert (eastings,northings):
    height = 0
    lat1 = E_N_to_Lat(eastings, northings, 6377563.396, 6356256.910, 400000, -100000, 0.999601272, 49.00000, -2.00000)
    lon1 = E_N_to_Long(eastings, northings, 6377563.396, 6356256.910, 400000, -100000, 0.999601272, 49.00000, -2.00000)
    x1 = Lat_Long_H_to_X(lat1, lon1, height, 6377563.396, 6356256.910)
    y1 = Lat_Long_H_to_Y(lat1, lon1, height, 6377563.396, 6356256.910)
    z1 = Lat_H_to_Z(lat1, height, 6377563.396, 6356256.910)
    x2 = Helmert_X(x1, y1, z1, 446.448, 0.2470, 0.8421, -20.4894)
    y2 = Helmert_Y(x1, y1, z1, -125.157, 0.1502, 0.8421, -20.4894)
    z2 = Helmert_Z(x1, y1, z1, 542.060, 0.1502, 0.2470, -20.4894)
    latitude = XYZ_to_Lat(x2, y2, z2, 6378137.000, 6356752.313)
    longitude = XYZ_to_Long(x2, y2)
    return (latitude, longitude)