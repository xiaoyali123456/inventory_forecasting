from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import random

movie_list = ['Arakshaka', 'Komedi Moderen Gokil', 'Maze', 'Chaipatti', 'Cara Majaka', 'SAPNEY', 'The Red Baron',
              'Garfield: A Tail Of Two Kitties', 'A Walk In The Clouds', 'White Fang', 'A Phone Call', 'Code Name: Geronimo',
              '1977 - Jarigindi Yemiti', 'Total Dadagiri', 'Northpole', 'Lt. Robin Crusoe, U.S.N.', 'Ishqedarriyaan - Trailer',
              'SANG MARTIR', 'Volver', 'The Immortal Life of Henrietta Lacks', 'Mud', 'Interstellar', 'Diary Of A Wimpy Kid',
              'Disney Dcom Upside Down Magic', 'Ennodu Modhi Paar', 'Jack and the Beanstalk', 'Maze Runner: The Death Cure',
              'The Shaggy D.A.', 'Trail of the Panda', 'The Lion King 1 1/2', 'Labuan Hati', 'Singularity', 'Raani', 'Women of 9/11',
              'Khiladi Lakshmana', 'LAMPOR Keranda Terbang', 'The Straight Story', 'Science Fair', 'Agyaat', 'CINTA DI SAKU CELANA',
              'NENEK GAYUNG', 'The Fisher King', 'Premikulu', 'Conspiracy Theory', 'Dayalu', 'From Hell', 'Aadat', 'The Real Jackpot',
              'Because of Winn-Dixie', 'Tokyo Mater', 'Catatan Si Boy 1', 'The Rocketeer', 'Ajnabee', 'Adventures of Tarzan', "Mirza's Friend Ghalib",
              'UP, UP AND AWAY (DISNEY CHANNEL)', 'Premium Rush', 'Guru', 'Kaadhalar Kudiyiruppu', 'Night Watch', 'Why Him?', 'Recess: All Growed Down',
              'Camouflage', 'Amanat', 'Baki Itihas', 'Anjaan', 'GIANT ROBBER CRAB, THE', 'The Host', 'Under Wraps', 'Sadugudu Vandi', 'The Last Rights',
              'Secrets of Life', 'Artemis Fowl', 'KHATTA MEETHA', 'Mehrooni', 'Morning Light', 'Stonehenge Decoded: Secrets Revealed', 'The Unknown',
              'Sang Pemimpi', 'Jaguar', 'Gabhricha Paus', 'Kucch Toh Hai', 'Red Stop', "Bob Ballard: An Explorer's Life", 'Ada Apa Dengan Pocong',
              'Fury', 'ALONG WITH THE GODS 2', 'Fantastic Four: Rise Of The Silver Surfer', 'Snowball Express', '12:00 AM', 'Milly & Mamet',
              'ISM', 'SABTU BERSAMA BAPAK', 'Street Light', 'Humraaz', 'DND - Jeritan Malam', 'The Coat', "Mangalyaan: India's Mission To Mars",
              'The Viagra Generation', 'Always Watching: A Marble Hornets Story']
movie_list = ['Docudrama', 'Food', 'Swimming', 'Sports', 'Wildlife', 'Arm Wrestling', 'Athletics', 'Mystery', 'Soap Opera / Melodrama',
              'Table Tennis', 'Musical', 'Concert Film', 'Baseball', 'Shorts', 'School Games', 'Mythology', 'Standup Comedy',
              'Sport', 'Action', 'Historical', 'Parody', 'Talk Show', 'Game Show / Competition', 'Film Noir', 'Romance', 'Thriller',
              'Awards', 'Formula1', 'Teen', 'Family', 'Fantasy', 'Action-Adventure', 'Legal', 'Mixed Martial Arts', 'Wrestling',
              'Animation', 'Kids', 'Medical', 'Anthology', 'Dance', 'Celebrities', 'News', 'Boxing', 'GOLF', 'Health and Fitness',
              'Science', 'Documentary', 'LiveTV', 'Drama', 'Travel', 'Badminton', 'Animals & Nature', 'Reality', 'Anime', 'Science Fiction',
              'Volleyball', 'American Football', 'Western', 'Comedy', 'Hockey', 'Tennis', 'Crime', 'Kabaddi', 'FORMULA2', 'Buddy', 'Survival',
              'Biographical', 'Water Sports', 'Disaster', 'Music', 'Coming of Age', 'Mini-series', 'Football', 'root', 'Superhero', 'Adventure',
              'Police/Cop', 'Basketball', 'Olympics', 'Biopic', 'Spy/Espionage', 'Romantic Comedy', 'Variety', 'Cricket', 'Lifestyle', 'Docuseries',
              'Music Video', 'ESPORTS', 'Horror', 'Procedural', 'Formula E']
# keyword_dic = {'drama': 50, 'comedy': 80, 'action': 20, 'horror': 5}
keyword_dic = {key: random.randint(1, 99) for key in movie_list}
wordcloud = WordCloud(width=800, height=800,
                      background_color='white',
                      min_font_size=5).generate_from_frequencies(keyword_dic)
plt.figure(figsize=(8, 8), facecolor=None)
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)
plt.show()
