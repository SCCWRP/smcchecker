from pyproj import Proj, transform
def convert_coor(xy_tup):
    inProj = Proj(init='epsg:3857')
    outProj = Proj(init='epsg:4326')
    x1,y1 = xy_tup[0],xy_tup[1]
    x2,y2 = transform(inProj,outProj,x1,y1)
    return (x2,y2)