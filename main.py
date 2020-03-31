import itertools
import matplotlib.pyplot as plt
import pandas as pd
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from multiprocessing import Pool
from urllib import request, parse


def main():
    pool = Pool(8)
    print('Started at: {}'.format(datetime.now().time()))
    # load raw html data pages
    raw_htmls = load_data(pool, [2018, 2019], ['M', 'W'], [6, 10, 21])
    print('Load completed at: {}'.format(datetime.now().time()))
    # parse table as numpy matrix
    raw_data = {key: parse_raw_htmls(pool, rh) for key, rh in raw_htmls.items()}
    print('Parsing completed at: {}'.format(datetime.now().time()))
    pool.terminate()
    # feed it to pandas and visualize:
    visualize('2T', raw_data)


def load_data(pool, years, sexes, distances):
    htmls = {}
    for y in years:
        for s in sexes:
            for d in distances:
                link = make_search_link(y, s, d, None)
                key_common = '{}-{}-{}'.format(y, d, s)
                key = '{}-1'.format(key_common)
                first_page = load_html((key, link))
                last_page_n = parse_last_page_number(first_page)
                links_to_process = []
                for i in range(last_page_n):
                    page = i + 1
                    key = '{}-{}'.format(key_common, page)
                    links_to_process.append((key, make_search_link(y, s, d, page)))
                htmls[key_common] = pool.map(load_html, links_to_process)
    return htmls


def make_search_link(year, sex, distance, page):
    """
    :param year: 2018, 2019
    :param sex: 'M', 'W'
    :param distance: 6, 10, 21
    :return: search link for specified params
    """
    baseLink = '''https://minskhalfmarathon.by/results/?year={}&last_name=&distance%5B0%5D={}&number_from=&number_to=&sex%5B0%5D={}&country=30&set_filter=%D0%9F%D0%BE%D0%BA%D0%B0%D0%B7%D0%B0%D1%82%D1%8C'''.format(year, distance, sex)
    if page:
        return '''{}&PAGEN_1={}'''.format(baseLink, page)
    else:
        return baseLink


def load_html(key_and_url):
    key, url = key_and_url
    file_name = os.path.join('html_cache', 'index.{}.html'.format(key))
    content = ''
    if os.path.exists(file_name):
        # print('Using cached data of ' + file_name)
        with open(file_name) as f:
            content = ''.join(f.readlines())
    else:
        req = request.Request(url)
        print('Making request... {}'.format(url))
        resp = request.urlopen(req)
        content_type = resp.headers['Content-Type']
        enc = content_type[content_type.find('charset=') + len('charset='):]
        decoded = [bs.decode(enc) for bs in resp.readlines()]
        resp.close()
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, 'w+') as f:
            for line in decoded:
                f.write(line)
        content = ''.join(decoded)
        # print('Response length:', len(content))
    return content


def parse_raw_htmls(pool, htmls):
    times = list(itertools.chain.from_iterable(pool.map(parse_raw_html, htmls)))
    results = pd.to_timedelta(times)
    return results


def parse_raw_html(html):
    times = []
    soup = BeautifulSoup(html, 'html.parser')
    members_div = soup.find(id='members')
    table = members_div.table
    thead = table.thead
    tbody = table.tbody
    for tr in tbody.find_all('tr'):
        cells = tr.find_all('td')
        time = cells[9].text
        if time.count(':') == 1:
            time = '00:' + time
        times.append(time)
    return times


def parse_last_page_number(html):
    soup = BeautifulSoup(html, 'html.parser')
    members_div = soup.find(id='members')
    members_siblings = members_div.parent.find_all('font')
    link = members_siblings[1].find(name='a', text='Конец')['href']
    pattern = 'PAGEN_1='
    page = link[link.rfind(pattern) + len(pattern):]
    return int(page)


def visualize(rounding, raw_data):
    grouped_data = {}
    for key, rd in raw_data.items():
        mean_time = timedelta(seconds=rd.mean().seconds)
        print('Mean time for {} ({} total): {}'.format(key, len(rd), mean_time))
        prepared = rd.round(rounding).to_frame()
        grouped = prepared.resample(rounding).count()
        grouped.index = grouped.index.map(lambda x: str(x).replace('0 days ', ''))
        grouped_data[key] = grouped
    fig, ax = plt.subplots()
    for label, gd in grouped_data.items():
        ax.plot(gd, marker='o', markersize=8, linestyle='-', label=label)
    ax.set_xlabel('Time of completion')
    ax.set_ylabel('Minsk Half-Marathon')
    ax.legend()
    plt.xticks(rotation=90)
    plt.show()


if __name__ == '__main__':
    main()
