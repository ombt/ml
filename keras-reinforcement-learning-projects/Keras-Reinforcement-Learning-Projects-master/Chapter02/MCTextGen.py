import markovify

with open("corpus.txt", "rb") as f:
    CorpusText = f.read()

utf8_text = CorpusText.decode("utf-8", "ignore")
ascii_data = utf8_text.encode("ascii", "ignore")

TextModel = markovify.Text(ascii_data)

print("Five randomly-generated sentences")
print("-----------------------------------")
for i in range(5):
    print(TextModel.make_sentence())

print("-----------------------------------")
print("three randomly-generated sentences of no more than 100 characters")
print("-----------------------------------")
for i in range(3):
    print(TextModel.make_short_sentence(100))
