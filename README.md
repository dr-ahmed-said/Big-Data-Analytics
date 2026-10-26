# Mariam Wael Hassan Mabrouk 231005860

my MapReduce-like engine written in Python It counts words in a folder of text files. 

## How it works

1. **Input:** every `.txt` file in the folder is one split.
2. **Map:** for each split, emit `(word, 1)` for every word (text is lowercased and punctuation removed).
3. **Parallel mappers:** multiprocessing
4. **Combine + Partition:** each mapper adds up its own counts, then sends each word to one of 2 reducers (words starting with a-f go to reducer 0, all others go to reducer 1).
5. **Shuffle & Sort, then Reduce:** each reducer collects its words from all mappers, groups them by word, sorts them, and sums the counts. One output file per reducer

## Sample input

3 claim notes, one per file:

```
car fire car damage
water roof water damage
car theft water damage
```

## Sample output

`part-00000` (words a-f):

```
car 3
damage 3
fire 1
```

`part-00001` (words g-z):

```
roof 1
theft 1
water 3
```

I also ran it on the 5 `book_0X.txt` files: it found 235 words in total 
