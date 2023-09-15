import asyncio
from pprint import pprint
import aiohttp
from more_itertools import chunked
from models import Base, SwapiPeople, engine, Session
import requests


def get_info(url_list):
    final_list = []
    for url in url_list:
        info_json = requests.get(url).json()
        first_key = list(info_json.keys())[0]
        final_list.append(info_json[first_key])
    return final_list


async def insert_in_db(people_list_json):

    people_list = [SwapiPeople(
        birth_year=person["birth_year"],
        eye_color=person["eye_color"],
        films=' , '.join(get_info(person["films"])),
        gender=person["gender"],
        hair_color=person["hair_color"],
        height=person["height"],
        homeworld=person["homeworld"],
        mass=person["mass"],
        name=person["name"],
        skin_color=person["skin_color"],
        species=' , '.join(get_info(person["species"])),
        starships=' , '.join(get_info(person["starships"])),
        vehicles=' , '.join(get_info(person["vehicles"]))
    ) for person in people_list_json]
    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def get_people(people_id):

    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.py4e.com/api/people/{people_id}")
    json_data = await response.json()
    await session.close()
    return json_data


async def main():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    for person_id_chunked in chunked(range(1, 100), 5):

        persons_coro = []
        for person_id in person_id_chunked:
            person_coro = get_people(person_id)
            persons_coro.append(person_coro)
        people = await asyncio.gather(*persons_coro)
        insert_in_db_coro = insert_in_db(people)
        asyncio.create_task(insert_in_db_coro)

    main_task = asyncio.current_task()
    insert_tasks = asyncio.all_tasks()
    insert_tasks.remove(main_task)
    await asyncio.gather(*insert_tasks)

    print('Done')


if __name__ == "__main__":
    asyncio.run(main())

