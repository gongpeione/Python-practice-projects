from PIL import Image
import argparse

asciiChar = list('$@B%8&WM#*oahkbdpqwmZO0QLCJUYXzcvunxrjft/\\|()1{}[]?-_+~<>i!lI;:,"^`\'. ')
asciiCharLength = len(asciiChar)

parser = argparse.ArgumentParser()

parser.add_argument('file')
parser.add_argument('-o', '--output')
parser.add_argument('--width', default = 80, type = int)
parser.add_argument('--height', default = 80, type = int)

args = parser.parse_args()

IMG_FILE    = args.file
IMG_WIDTH   = args.width
IMG_HEIGHT  = args.height
FILE_OUTPUT = args.output

def rgb2Char( r, g, b, alpha = 256 ) :

    if alpha == 0 :
        return ' '
    
    rgb2Gray   = int( 0.2126 * r + 0.7152 * g + 0.0722 * b )
    gray2Order = int( rgb2Gray / (256.0 / asciiCharLength) )

    return asciiChar[ gray2Order ]

if __name__ == '__main__' :
    # print(IMG_FILE, IMG_WIDTH, IMG_HEIGHT, FILE_OUTPUT)
    img = Image.open(IMG_FILE)
    img = img.resize((IMG_WIDTH, IMG_HEIGHT), Image.NEAREST).convert('RGBA')

    rgbList = ''
    for column in range(IMG_HEIGHT) :
        for row in range(IMG_WIDTH) :
            # print(str(img.getpixel( (row, column) )))
            rgbList += rgb2Char( *img.getpixel( (row, column) ) )
        rgbList += '\n'

    print(rgbList)

    if FILE_OUTPUT :
        with open(FILE_OUTPUT, 'w') as file :
            file.write(rgbList)