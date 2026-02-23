import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys

# Uso: python plot_results.py resultados.csv

if len(sys.argv) < 2:
    print("❌ Error: Debes indicar el fichero CSV como argumento.")
    print("   Uso: python3 plot_results.py <resultados.csv>")
    sys.exit(1) # Salimos con código de error

file_path = sys.argv[1]

try:
    df = pd.read_csv(file_path)
except Exception as e:
    print(f"❌ Error leyendo el CSV: {e}")
    sys.exit(1)

# Convertir Bytes a KB/MB para lectura
def format_bytes(size):
    if size >= 1024**2: return f"{size/1024**2:.0f}MB"
    if size >= 1024: return f"{size/1024:.0f}KB"
    return f"{size}B"

try:
    data_hostname = df['Hostname'].iloc[0]
    data_username = df['Username'].iloc[0]
    data_timestamp = df['Timestamp'].iloc[0]
except KeyError:
    data_hostname = "UNKNOWN"
    data_username = "UNKNOWN"
    data_timestamp = "UNKNOWN"
except IndexError:
    print("❌ El CSV está vacío.")
    sys.exit(1)

df['SizeLabel'] = df['MsgSize'].apply(format_bytes)
# Filtrar colectivas únicas
collectives = df['Collective'].unique()

print(f"--- Generando gráficas para: {data_username}@{data_hostname} ---")

for coll in collectives:
    plt.figure(figsize=(10, 6))
    subset = df[df['Collective'] == coll]
    
    # Gráfica de Líneas: Eje X = MsgSize, Eje Y = TimeMax
    sns.lineplot(data=subset, x='MsgSize', y='TimeMax', hue='Implementation', marker='o')
    
    plt.xscale('log')
    plt.yscale('log') # Escala logarítmica es vital para ver latencia y ancho de banda a la vez
    plt.title(f"Performance: {coll} ({subset['Processes'].iloc[0]} procs)")
    plt.ylabel("Time (seconds) [Log Scale]")
    plt.xlabel("Message Size (Bytes)")
    plt.grid(True, which="both", ls="-", alpha=0.2)
    
    watermark = f"{data_username}@{data_hostname}\n{data_timestamp}"
    plt.text(0.5, 0.5, watermark, 
            fontsize=30, color='gray', 
            ha='center', va='center', alpha=0.15, 
            transform=plt.gca().transAxes, rotation=30)
 
    footer_text = f"User: {data_username} | Host: {data_hostname}"
    plt.figtext(0.99, 0.01, footer_text, 
                ha="right", fontsize=8, color="red")
                
    output_filename = f"{coll}.png"
    plt.savefig(output_filename)
    print(f"  -> Generado: {output_filename}")
    plt.close()

print("✅ Proceso completado.")
