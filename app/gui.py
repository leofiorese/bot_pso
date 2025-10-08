import tkinter as tk
from tkinter import messagebox
from tkinter import simpledialog
from app.main import run_once  # Importando a função da main.py
import time
import threading

# Função para rodar o processo em segundo plano (evitar travar a interface)
def run_process_in_thread():
    try:
        run_once()
        messagebox.showinfo("Sucesso", "Processo concluído com sucesso!")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {e}")

# Função para exibir a janela de data personalizada
def ask_for_custom_date():
    def on_submit():
        # Obter as respostas
        custom_date = date_var.get().lower()
        
        if custom_date == "sim":
            try:

                days_value = int(days_entry.get())

                if days_value <= 0:
                    messagebox.showerror("Erro", "O número de dias deve ser um valor positivo.")

                    return 
                    
                messagebox.showinfo("Data personalizada", f"Você escolheu: {days_value} dias.")

                run_process_in_thread()

            except ValueError:
                messagebox.showerror("Erro", "Digite um número válido para os dias.")

        
        elif custom_date == "não":
            messagebox.showinfo("Data personalizada", "Você escolheu: Não usar data personalizada.")

            run_process_in_thread()
        
        else:
            messagebox.showerror("Erro", "Por favor, responda com 'sim' ou 'não'.")
    
    # Criando a janela de confirmação de data personalizada
    custom_date_window = tk.Toplevel(root)
    custom_date_window.title("Escolha a data personalizada")
    custom_date_window.geometry("400x250")
    
    tk.Label(custom_date_window, text="Deseja usar uma data personalizada? (sim/não)").pack(pady=10)
    date_var = tk.StringVar()
    tk.Entry(custom_date_window, textvariable=date_var).pack(pady=10)
    
    tk.Label(custom_date_window, text="Informe o número de dias:").pack(pady=5)
    days_entry = tk.Entry(custom_date_window)
    days_entry.pack(pady=5)
    
    submit_button = tk.Button(custom_date_window, text="Confirmar", command=on_submit)
    submit_button.pack(pady=10)

# Criando a interface gráfica principal
root = tk.Tk()
root.title("PSO Bot Interface")
root.geometry("600x400")

# Título
tk.Label(root, text="PSO Bot", font=("Arial", 16)).pack(pady=20)

# Botão para iniciar o processo
run_button = tk.Button(root, text="Iniciar Processo", width=20, height=2, command=ask_for_custom_date)
run_button.pack(pady=20)

# Executando a interface gráfica
root.mainloop()
