import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

def scrape_story(story_id):
    """Scraping de una historia individual"""
    url = f'https://www.yourghoststories.com/real-ghost-story.php?story={story_id}'
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extraer título
        title_elem = soup.select('.storytitle')
        if not title_elem:
            return None
        title = title_elem[0].text.strip()
        
        # Extraer story info
        story_info = soup.select('.storyinfo')
        if len(story_info) < 2:
            return None
        
        raw_text = story_info[1].text
        
        # Regex corregidos
        country = re.search(r'Country:\s*([^\n]*?)(?=State:|Paranormal Category:|$)', raw_text)
        state = re.search(r'State:\s*([^\n]*?)(?=Paranormal Category:|$)', raw_text)
        category = re.search(r'Paranormal Category:\s*([^\n]+)', raw_text)
        
        # Extraer historia
        story_paras = soup.select('div#story p')
        story_text = '\n\n'.join([p.text for p in story_paras])
        
        return {
            'story_id': story_id,
            'title': title,
            'country': country.group(1).strip() if country else None,
            'state': state.group(1).strip() if state else None,
            'category': category.group(1).strip() if category else None,
            'story': story_text
        }
        
    except Exception as e:
        return None

def main():
    print("Iniciando scraping de 15,000 páginas...")
    print("Esto puede tomar varios minutos...")
    
    results = []
    start_time = time.time()
    
    # Usar ThreadPoolExecutor - ajustado para 15,000
    # Puedes aumentar max_workers si tu conexión lo permite
    with ThreadPoolExecutor(max_workers=30) as executor:
        # Crear tareas para 15,000 páginas
        futures = {executor.submit(scrape_story, i): i for i in range(1, 15001)}
        
        # Procesar con barra de progreso
        for future in tqdm(as_completed(futures), total=15000, desc="Scraping"):
            result = future.result()
            if result:
                results.append(result)
            
            # Guardar parcialmente cada 1000 historias (por si acaso)
            if len(results) % 1000 == 0:
                temp_df = pd.DataFrame(results)
                temp_df.to_csv('ghost_stories_temp.csv', index=False, encoding='utf-8')
                print(f"\n💾 Guardado temporal: {len(results)} historias")
    
    # Crear DataFrame final
    df = pd.DataFrame(results)
    
    # Ordenar por story_id
    df = df.sort_values('story_id').reset_index(drop=True)
    
    # Limpiar valores vacíos
    df['country'] = df['country'].str.strip()
    df['state'] = df['state'].str.strip()
    df['state'] = df['state'].replace('', None)
    
    # Guardar archivo final
    df.to_csv('ghost_stories_15000.csv', index=False, encoding='utf-8')
    
    # Tiempo total
    elapsed_time = time.time() - start_time
    
    print(f"\n✅ Scraping completado!")
    print(f"📊 Total de historias encontradas: {len(df)} de 15,000")
    print(f"⏱️ Tiempo total: {elapsed_time/60:.2f} minutos")
    print(f"💾 Guardado en: ghost_stories_15000.csv")
    
    # Mostrar estadísticas
    print(f"\n📈 Estadísticas:")
    print(f"- Historias con país: {df['country'].notna().sum()}")
    print(f"- Historias con estado: {df['state'].notna().sum()}")
    print(f"- Categorías únicas: {df['category'].nunique()}")
    print(f"- Países únicos: {df['country'].nunique()}")
    
    # Mostrar muestra
    print("\nPrimeras 5 historias:")
    print(df[['story_id', 'title', 'country', 'category']].head())
    
    print("\nÚltimas 5 historias:")
    print(df[['story_id', 'title', 'country', 'category']].tail())
    
    return df

if __name__ == "__main__":
    df = main()