require "csv"
require "pry"
require "pg"

###############################
########database methods########
###############################


def db_connection
  begin
    connection = PG.connect(dbname: "korning")
    yield(connection)
  ensure
    connection.close
  end
end

def insert_employee(employee_info)
  db_connection do |conn|
    # query database to see if this employee already exists
    result = conn.exec_params("SELECT id, name FROM employees WHERE name = $1", [employee_info[:name]]).first
    if result.nil?
      # no rows with that name exist, insert the employee and return the id
      conn.exec_params('INSERT INTO employees (name, email) VALUES ($1, $2)',[employee_info[:name], employee_info[:email]])
      result = conn.exec_params("SELECT id, name FROM employees WHERE name = $1", [employee_info[:name]]).first
    end
    result = { result["name"] => result["id"] }
  end
end

def insert_customers(customer)
  db_connection do |conn|
    customer_result = conn.exec_params("SELECT id, name FROM customers WHERE name = $1", [customer[:name]]).first
    if customer_result.nil?
      conn.exec_params('INSERT INTO customers (name, account_num) VALUES ($1, $2)', [customer[:name], customer[:account_num].to_i])
      customer_result = conn.exec_params("SELECT id, name FROM customers WHERE name = $1", [customer[:name]]).first
    end
    customer_result = { customer_result["name"] => customer_result["id"] }
  end
end

def insert_sale(sale_info, product_id, invoice_id, customer_id, employee_id)
  db_connection do |conn|
    conn.exec_params('INSERT INTO sales (sale_date, sale_amount, units_sold, invoice_num, product_id, invoice_id, customer_id, employee_id)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', [sale_info[:sale_date], sale_info[:sale_amount], sale_info[:units_sold], sale_info[:invoice_no], product_id.values.first, invoice_id.values.first, customer_id.values.first, employee_id.values.first ])
  end
end

def insert_products(product_items)
  db_connection do |conn|
    result = conn.exec_params("SELECT id, product FROM products WHERE product = $1", [product_items[:item]]).first
    if result.nil?
      conn.exec_params('INSERT INTO products (product) VALUES ($1)', [product_items[:item]])
      result = conn.exec_params("SELECT id, product FROM products WHERE product = $1", [product_items[:item]]).first
    end
    result = { result["product"] => result["id"] }
  end
end

def insert_invoice_frequency(invoice_frequency)
  db_connection do |conn|
    result = conn.exec_params("SELECT id, invoice_frequency FROM invoices WHERE invoice_frequency = $1", [invoice_frequency[:frequency]]).first
    if result.nil?
      conn.exec_params('INSERT INTO invoices (invoice_frequency) VALUES ($1)',[invoice_frequency[:frequency]])
      result = conn.exec_params("SELECT id, invoice_frequency FROM invoices WHERE invoice_frequency = $1", [invoice_frequency[:frequency]]).first
    end
    result = { result["invoice_frequency"] => result["id"] }
  end
end

###############################
########parsing methods########
###############################

def parse_employee(row)
  employee = {}
  employee[:name] = row['employee'].split(' ')[0..1].join(' ')
  employee[:email] = row['employee'].split(' ')[2].gsub(/[()]/,"")
  employee
end


def parse_customer(row)
  customer = {}
  customer[:name] = row['customer_and_account_no'].split(' ')[0]
  customer[:account_num] = row['customer_and_account_no'].split(' ')[1].gsub(/[()]/,"")
  customer
end

def parse_invoice_frequency(row)
  invoice = {}
  invoice[:frequency] = row["invoice_frequency"]
  invoice
end

def parse_product_items(row)
  products = {}
  products[:item] = row["product_name"]
  products
end

def parse_sale_info(row)
  sale_info = {}
  sale_info[:sale_date] = row["sale_date"]
  sale_info[:sale_amount] = row["sale_amount"].split('')[1..-1].join('').to_f
  sale_info[:units_sold] = row["units_sold"]
  sale_info[:invoice_no] = row["invoice_no"]
  sale_info
end



def csv_to_db
  CSV.foreach("sales.csv", headers: true) do |row|
    employee = parse_employee(row) #return a hash
    customer = parse_customer(row) #return a hash
    invoice_frequency = parse_invoice_frequency(row)
    product_items = parse_product_items(row)

    sale_info = parse_sale_info(row)

    employee_id = insert_employee(employee) #run hash from above through insert employee method to return hash where name is associated with id
    customer_id = insert_customers(customer)
    product_id = insert_products(product_items)
    invoice_id  = insert_invoice_frequency(invoice_frequency)

    insert_sale(sale_info, product_id, invoice_id, customer_id, employee_id)
  end
end

csv_to_db
