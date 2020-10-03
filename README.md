# Map-Reduce_pixel_histogram
The purpose of this project is to develop a simple Map-Reduce program on Hadoop that creates histograms of pixels


A pixel in an image can be represented using 3 colors: red, green, and blue, where each color intensity is an integer between 0 and 255. In this project I have written a Map-Reduce program that derives a histogram for each color. For red, for example, the histogram will indicate how many pixels in the dataset have a green value equal to 0, equal to 1, etc (256 values). The pixel file is a text file that has one text line for each pixel. 

For example, the line
```
23,140,45
```
represents a pixel with red=23, green=140, and blue=45.


Map-Reduce job is in the Java file src/main/java/Histogram.java. 



To help you understand, I am giving you the pseudo code:
```
class Color {
    public short type;       /* red=1, green=2, blue=3 */
    public short intensity;  /* between 0 and 255 */
}
map ( key, line ):
  read 3 numbers from the line and store them in the variables red, green, and blue. Each number is between 0 and 255.
  emit( Color(1,red), 1 )
  emit( Color(2,green), 1 )
  emit( Color(3,blue), 1 )

reduce ( color, values )
  sum = 0
  for ( v in values )
      sum += v
  emit( color, sum )
```
In Java main program args[0] is the file with the pixels (pixels-small.txt or pixels-large.txt) and args[1] is the output directory.
