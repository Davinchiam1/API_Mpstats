import tkinter as tk
import os
from tkinter import filedialog
from tkinter import messagebox
from API_Mpstats import requ_Mpstats
from tkcalendar import DateEntry
from db_wb import update_brands


class App(tk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.master = master
        self.grid()
        self.create_widgets()

    def create_widgets(self):
        """Main window with load from api and load from files blocks"""

        # save directory entry
        self.save_label = tk.Label(self, text="Save directory:")
        self.save_label.grid(row=0, column=0, sticky=tk.W)
        self.save_entry = tk.Entry(self)
        self.save_entry.grid(row=0, column=1)
        self.save_button = tk.Button(self, text="Browse...", command=self.browse_save_directory)
        self.save_button.grid(row=0, column=2)

        # choose type of marketplace (Wildberries or Ozon)
        self.wb_oz_var = tk.StringVar()
        self.wb_oz_var.set("wb")
        self.rb01 = tk.Radiobutton(self, text="Wildberries", variable=self.wb_oz_var, value="wb")
        self.rb02 = tk.Radiobutton(self, text="Ozon", variable=self.wb_oz_var, value="oz")
        self.rb01.grid(row=0, column=3, sticky=tk.N)
        self.rb02.grid(row=0, column=4, sticky=tk.N)

        # load data from category
        self.load_button = tk.Button(self, text="Load category", command=self.load_category)
        self.load_button.grid(row=3, column=1)

        # start date entry
        self.date_entry_start = DateEntry(self, width=12, date_pattern='yyyy-MM-dd', background='darkblue',
                                          foreground='white', borderwidth=2, year=2021)
        self.date_entry_start.grid(row=1, column=2)

        # end date entry
        self.date_entry_end = DateEntry(self, width=12, date_pattern='yyyy-MM-dd', background='darkblue',
                                        foreground='white', borderwidth=2, year=2023)
        self.date_entry_end.grid(row=1, column=3)

        # category entry
        self.category_label = tk.Label(self, text="Category:")
        self.category_label.grid(row=1, column=0, sticky=tk.W)
        self.category_entry = tk.Entry(self)
        self.category_entry.grid(row=1, column=1)

        # separate files
        self.sep_files_var = tk.BooleanVar()
        self.sep_files_checkbox = tk.Checkbutton(self, text="Separate files", variable=self.sep_files_var)
        self.sep_files_checkbox.grid(row=1, column=4, sticky=tk.W)

        # quit button
        self.quit_button = tk.Button(self, text="Quit", command=self.master.destroy)
        self.quit_button.grid(row=3, column=2)

        # button for new window for sku loading
        self.new_window_button = tk.Button(self, text="SKU search", command=self.open_new_window)
        self.new_window_button.grid(row=3, column=3)

        # button for new window for sku loading
        self.brand_window_button = tk.Button(self, text="Brand load", command=self.open_brand_load_window)
        self.brand_window_button.grid(row=3, column=4)

    def open_new_window(self):
        """Secondary window for load from api by sku"""
        new_window = tk.Toplevel(self.master)
        new_window.title("SKU search")
        new_window.geometry("350x200")

        # browsing sku file or entering the sku
        self.SKU = tk.Label(new_window, text="SKU entry:")
        self.SKU.grid(row=1, column=0, sticky=tk.W)
        self.SKU_entry = tk.Entry(new_window)
        self.SKU_entry.grid(row=1, column=1)
        self.SKU_button = tk.Button(new_window, text="Browse...", command=self.browse_SKU_file)
        self.SKU_button.grid(row=1, column=2)

        # choose the marketplace
        self.rb01 = tk.Radiobutton(new_window, text="Wildberries", variable=self.wb_oz_var, value="wb")
        self.rb02 = tk.Radiobutton(new_window, text="Ozon", variable=self.wb_oz_var, value="oz")
        self.rb01.grid(row=0, column=0, sticky=tk.N)
        self.rb02.grid(row=0, column=1, sticky=tk.N)

        # save directory browse
        self.save_label = tk.Label(new_window, text="Save directory:")
        self.save_label.grid(row=3, column=0, sticky=tk.W)
        self.save_entry = tk.Entry(new_window)
        self.save_entry.grid(row=3, column=1)
        self.save_button = tk.Button(new_window, text="Browse...", command=self.browse_save_directory)
        self.save_button.grid(row=3, column=2)

        # start date entry
        self.date_entry_start = DateEntry(new_window, width=12, date_pattern='yyyy-MM-dd', background='darkblue',
                                          foreground='white', borderwidth=2, year=2023)
        self.date_entry_start.grid(row=2, column=0)

        # end date entry
        self.date_entry_end = DateEntry(new_window, width=12, date_pattern='yyyy-MM-dd', background='darkblue',
                                        foreground='white', borderwidth=2, year=2023)
        self.date_entry_end.grid(row=2, column=1)

        # load info checkbox
        self.load_info_var = tk.BooleanVar()
        self.load_info_checkbox = tk.Checkbutton(new_window, text="Load info", variable=self.load_info_var)
        self.load_info_checkbox.grid(row=4, column=0, sticky=tk.W)

        # load sales checkbox
        self.load_sales_var = tk.BooleanVar()
        self.load_sales_checkbox = tk.Checkbutton(new_window, text="Load sales", variable=self.load_sales_var)
        self.load_sales_checkbox.grid(row=4, column=1, sticky=tk.W)

        # database connect checkbox
        self.db_connect_var = tk.BooleanVar()
        self.db_connect_checkbox = tk.Checkbutton(new_window, text="Load to database", variable=self.db_connect_var)
        self.db_connect_checkbox.grid(row=5, column=0, sticky=tk.W)

        # load data by SKU button
        self.load_SKU_button = tk.Button(new_window, text="Load SKU", command=self.load_SKU)
        self.load_SKU_button.grid(row=4, column=2)

        # load handbook data to database
        self.load_to_db = tk.Button(new_window, text="Load handbook", command=self.load_to_db)
        self.load_to_db.grid(row=5, column=2)

    def open_brand_load_window(self):
        """Secondary window for load from api by brand"""
        new_window = tk.Toplevel(self.master)
        new_window.title("Brand load")
        new_window.geometry("350x150")

        # entering the brand
        self.brand = tk.Label(new_window, text="Brand entry:")
        self.brand.grid(row=1, column=0, sticky=tk.W)
        self.brand_entry = tk.Entry(new_window)
        self.brand_entry.grid(row=1, column=1)

        # choose the marketplace
        self.rb01 = tk.Radiobutton(new_window, text="Wildberries", variable=self.wb_oz_var, value="wb")
        self.rb02 = tk.Radiobutton(new_window, text="Ozon", variable=self.wb_oz_var, value="oz")
        self.rb01.grid(row=0, column=0, sticky=tk.N)
        self.rb02.grid(row=0, column=1, sticky=tk.N)

        # save directory browse
        self.save_label = tk.Label(new_window, text="Save directory:")
        self.save_label.grid(row=3, column=0, sticky=tk.W)
        self.save_entry = tk.Entry(new_window)
        self.save_entry.grid(row=3, column=1)
        self.save_button = tk.Button(new_window, text="Browse...", command=self.browse_save_directory)
        self.save_button.grid(row=3, column=2)

        # start date entry
        self.date_entry_start = DateEntry(new_window, width=12, date_pattern='yyyy-MM-dd', background='darkblue',
                                          foreground='white', borderwidth=2, year=2023)
        self.date_entry_start.grid(row=2, column=0)

        # end date entry
        self.date_entry_end = DateEntry(new_window, width=12, date_pattern='yyyy-MM-dd', background='darkblue',
                                        foreground='white', borderwidth=2, year=2023)
        self.date_entry_end.grid(row=2, column=1)

        # load data by brand button
        self.load_brand_button = tk.Button(new_window, text="Load brand", command=self.load_brand)
        self.load_brand_button.grid(row=4, column=2)

        self.db_connect_var = tk.BooleanVar()
        self.db_connect_checkbox = tk.Checkbutton(new_window, text="Load to database", variable=self.db_connect_var)
        self.db_connect_checkbox.grid(row=4, column=0, sticky=tk.W)



    def load_category(self):
        """Execute load category by name from d1 to d2. Category must be named in style - Base_name/Secondary_name/...
        Category can be loaded from Ozon or Wildberries"""
        save_directory = self.save_entry.get()
        category = self.category_entry.get()
        d1 = self.date_entry_start.get()
        d2 = self.date_entry_end.get()
        sep_files = self.sep_files_var.get()
        if self.wb_oz_var.get() == 'wb':
            api_connect = requ_Mpstats()
        else:
            api_connect = requ_Mpstats(request='oz')
        if category is not None:
            api_connect.get_cat_by_dates(category_string=category, start_date=d1, end_date=d2,
                                         save_directory=save_directory, separate_files=sep_files)

    def browse_save_directory(self):
        """Get dir to save loaded files"""
        save_directory = filedialog.askdirectory()
        self.save_entry.delete(0, tk.END)
        self.save_entry.insert(0, save_directory)

    def load_to_db(self):
        """Load all SKU to db"""
        pass

    def load_brand(self):
        """Load items by brand"""
        brand = self.brand_entry.get()
        db_connect = self.db_connect_var.get()
        d1 = self.date_entry_start.get()
        d2 = self.date_entry_end.get()
        save_directory = self.save_entry.get()
        wb_oz_var=self.wb_oz_var.get()
        if wb_oz_var == 'wb':
            api_connect = requ_Mpstats()
        else:
            api_connect = requ_Mpstats(request='oz')
        if brand is not None:
            if db_connect:
                update_brands(table_name="brand_"+brand,brand_string=brand, startdate=d1, enddate=d2,wb_oz_var=wb_oz_var)
            else:
                api_connect.get_brand_by_dates(brand_string=brand, start_date=d1, end_date=d2,
                                               save_directory=save_directory, db_connect=db_connect)

    def browse_SKU_file(self):
        """Get file with list of SKU to load from api"""
        filetypes = [("CSV files", "*.csv"), ("XLSX files", "*.xlsx")]
        directory = filedialog.askopenfilenames(initialdir=".", filetypes=filetypes)
        self.SKU_entry.delete(0, tk.END)
        self.SKU_entry.insert(0, directory)


    def load_SKU(self):
        """Load SKU info from api. Requires file with SKU list or single SKU. """
        save_directory = self.save_entry.get() + '\\SKU_list'
        if not os.path.exists(os.path.normpath((save_directory))):
            os.makedirs(save_directory)
        SKU_list = self.SKU_entry.get()
        db_connect = self.db_connect_var.get()
        d1 = self.date_entry_start.get()
        d2 = self.date_entry_end.get()
        if self.wb_oz_var.get() == 'wb':
            api_connect = requ_Mpstats()
        else:
            api_connect = requ_Mpstats(request='oz')
        if SKU_list is not None:
            if db_connect:
                self.frame = api_connect.load_by_SKU(sku_list=SKU_list, start_date=d1, end_date=d2,
                                                     save_directory=save_directory, load_info=self.load_info_var.get(),
                                                     load_sales=self.load_sales_var.get(), db_connect=db_connect)
            else:
                api_connect.load_by_SKU(sku_list=SKU_list, start_date=d1, end_date=d2, save_directory=save_directory,
                                        load_info=self.load_info_var.get(), load_sales=self.load_sales_var.get())
        print('Finished')



root = tk.Tk()
root.title("Data Processing App")
root.geometry("550x100")
root.resizable(False, False)
root.columnconfigure(3, minsize=50, weight=1)
root.columnconfigure(1, minsize=50, weight=1)

app = App(master=root)
app.mainloop()
