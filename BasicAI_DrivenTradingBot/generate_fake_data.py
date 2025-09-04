import random
from faker import Faker

# Initialize the Faker library to generate fake data
fake = Faker()

def generate_large_xml_file(num_books=10000, output_filename="large_books.xml"):
    """
    Generates a large XML file with a specified number of book records.
    """
    print(f"Generating {num_books} book records...")
    
    # Start the XML file with the root element
    xml_content = ['<catalog>']

    for i in range(num_books):
        book_id = f"bk{101 + i}"
        
        # --- Generate Book Details ---
        author_first = fake.first_name().replace("'", "")
        author_last = fake.last_name().replace("'", "")
        title = fake.catch_phrase().replace('&', 'and')
        genre = random.choice(["Fantasy", "Science Fiction", "Technical", "History", "Mystery", "Romance", "Horror"])
        price = round(random.uniform(7.99, 99.99), 2)
        publish_date = fake.date_this_century()

        # --- Generate Reviewers (0 to 5 reviewers per book) ---
        reviewers_xml = []
        for _ in range(random.randint(0, 5)):
            reviewer_name = fake.name().replace("'", "")
            # Generate a long, multi-sentence comment
            comment = fake.paragraph(nb_sentences=random.randint(2, 8)).replace('&', 'and')
            stars = random.randint(1, 5)
            # Add some special characters randomly to test cleaning
            if random.random() < 0.1:
                comment += " Highly recommendéd! ★"
            reviewers_xml.append(f'<reviewer stars="{stars}"><name>{reviewer_name}</name><comment>{comment}</comment></reviewer>')
        
        # --- Generate other array columns (0 to 4 items each) ---
        onemorecol_xml = [f"<col1>{fake.word()}</col1>" for _ in range(random.randint(0, 4))]
        secondcol_xml = [f"<col1>{fake.word()}</col1>" for _ in range(random.randint(0, 4))]

        # --- Assemble the complete <book> element ---
        book_entry = f"""
    <book id="{book_id}">
        <author>
            <name><first>{author_first}</first><last>{author_last}</last></name>
        </author>
        <title>{title}</title>
        <genre>{genre}</genre>
        <price>{price}</price>
        <publish_date>{publish_date}</publish_date>
        <reviewers>
            {''.join(reviewers_xml)}
        </reviewers>
        <onemorecolumn>
            {''.join(onemorecol_xml)}
        </onemorecolumn>
        <secondcoloum>
            {''.join(secondcol_xml)}
        </secondcoloum>
    </book>"""
        xml_content.append(book_entry)

    # Close the root element
    xml_content.append('</catalog>')
    
    # Write to file
    with open(output_filename, "w", encoding="utf-8") as f:
        f.write("\n".join(xml_content))
        
    print(f"Successfully created '{output_filename}' with {num_books} records.")

if __name__ == "__main__":
    NUM_BOOKS_TO_GENERATE = 1000000  # <--- Change this number as needed
    generate_large_xml_file(NUM_BOOKS_TO_GENERATE)