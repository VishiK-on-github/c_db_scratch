describe 'database' do
    def run_script(commands)
        raw_output = nil
        IO.popen("./bin/db", "r+") do |pipe|
            commands.each do |command|
                pipe.puts command
            end

            pipe.close_write

            # read the whole output
            raw_output = pipe.gets(nil)
        end
        raw_output.split("\n")
    end

    it 'inserts and retrieves a row' do
        result = run_script([
            "insert 1 user1 abcd@vishu.com",
            "select",
            ".exit"
        ])

        expect(result).to match_array([
            "db > Executed.",
            "db > (1, user1, abcd@vishu.com)",
            "Executed.",
            "db > ",
        ])
    end
    
    it 'prints error when table is full' do
        script = (1..1401).map do |i|
            "insert #{i} user#{i} person#{i}@example.com"
        end
        script << ".exit"
        result = run_script(script)
        expect(result[-2]).to eq('db > Error: Table full.')
    end

end